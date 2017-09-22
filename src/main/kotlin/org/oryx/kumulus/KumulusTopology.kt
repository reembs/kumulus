package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.Constants
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.oryx.kumulus.collector.KumulusBoltCollector
import org.oryx.kumulus.collector.KumulusSpoutCollector
import org.oryx.kumulus.component.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

class KumulusTopology(
        private val components: List<KumulusComponent>,
        private val componentInputs: MutableMap<Pair<String, GlobalStreamId>, Grouping>,
        val config: Map<String, Any>
) : KumulusEmitter {
    private val boltExecutionPool: ThreadPoolExecutor = ThreadPoolExecutor(
            (config[CONF_THREAD_POOL_CORE_SIZE] as? Long ?: 4L).toInt(),
            (config[CONF_THREAD_POOL_MAX_SIZE] as? Long ?: 10L).toInt(),
            config[CONF_THREAD_POOL_MAX_SIZE] as? Long ?: 20L,
            TimeUnit.SECONDS,
            ArrayBlockingQueue((config[CONF_THREAD_POOL_KEEP_ALIVE] as? Long)?.toInt() ?: 2000)
    )
    private val maxSpoutPending: Long
    private val mainQueue = LinkedBlockingQueue<KumulusMessage>()
    private val acker: KumulusAcker
    private val rejectedExecutionHandler = RejectedExecutionHandler { _, _ ->
        logger.error { "Execution was rejected" }
    }
    private val tickExecutor = ScheduledThreadPoolExecutor(1, rejectedExecutionHandler)
    private val started = AtomicBoolean(false)
    private val systemComponent = components.first { it.taskId() == Constants.SYSTEM_TASK_ID.toInt() }
    private val shutDownHook = CountDownLatch(1)

    init {
        this.boltExecutionPool.prestartAllCoreThreads()
        this.maxSpoutPending = config[org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING] as Long? ?: 0L
        this.acker = KumulusAcker(
                this,
                maxSpoutPending,
                config[CONF_EXTRA_ACKING] as? Boolean ?: false
        )
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        @JvmField
        val CONF_EXTRA_ACKING = "kumulus.allow-extra-acking"
        @JvmField
        val CONF_THREAD_POOL_KEEP_ALIVE = "kumulus.thread_pool.keep_alive_secs"
        @JvmField
        val CONF_THREAD_POOL_MAX_SIZE = "kumulus.thread_pool.max_size"
        @JvmField
        val CONF_THREAD_POOL_CORE_SIZE = "kumulus.thread_pool.core_pool_size"
    }

    fun prepare() {
        startQueuePolling()

        components.forEach { component ->
            val componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>> =
                    componentInputs
                            .filter { it.key.second._componentId == component.name() }
                            .map { Pair(it.key.second._streamId, Pair(it.key.first, it.value)) }

            mainQueue.add(when (component) {
                is KumulusSpout ->
                    SpoutPrepareMessage(
                            component, KumulusSpoutCollector(component, componentRegisteredOutputs, this, acker))
                is KumulusBolt -> {
                    BoltPrepareMessage(
                            component, KumulusBoltCollector(component, componentRegisteredOutputs, this, acker))
                }
                else ->
                    throw UnsupportedOperationException()
            })

            if (component is KumulusBolt) {
                component.tickSecs?.toLong()?.let { tickSecs ->
                    tickExecutor.scheduleWithFixedDelay({
                        if (started.get()) {
                            try {
                                val tuple = KumulusTuple(
                                        systemComponent,
                                        Constants.SYSTEM_TICK_STREAM_ID,
                                        listOf(),
                                        null
                                )
                                mainQueue.add(ExecuteMessage(component, tuple))
                            } catch (e: Exception) {
                                logger.error(e) { "Error in sending tick tuple" }
                            }
                        }
                    }, tickSecs, tickSecs, TimeUnit.SECONDS)
                }
            }
        }
    }

    private fun startQueuePolling() {
        val pollerRunnable = {
            while (true) {
                val message = mainQueue.take()!!
                val c = message.component
                if (c.inUse.compareAndSet(false, true)) {
                    boltExecutionPool.execute({
                        try {
                            when (message) {
                                is PrepareMessage<*> -> {
                                    if (c.isSpout())
                                        (c as KumulusSpout).prepare(message.collector as KumulusSpoutCollector)
                                    else
                                        (c as KumulusBolt).prepare(message.collector as KumulusBoltCollector)
                                }
                                is ExecuteMessage -> {
                                    assert(!c.isSpout()) {
                                        logger.error {
                                            "Execute message got to a spout '${c.context.thisComponentId}', " +
                                                    "this shouldn't happen."
                                        }
                                    }
                                    (c as KumulusBolt).execute(message.tuple)
                                }
                                else ->
                                    throw UnsupportedOperationException("Operation of type ${c.javaClass.canonicalName} is unsupported")
                            }
                        } finally {
                            c.inUse.set(false)
                        }
                    })
                } else {
                    logger.debug { "Component ${c.context.thisComponentId}/${c.taskId()} is currently busy" }
                    mainQueue.add(message)
                }
            }
        }

        Thread(pollerRunnable).apply {
            this.isDaemon = true
            this.start()
        }
    }

    fun start(block: Boolean = false) {
        val spouts = components.filter { it is KumulusSpout }.map { it as KumulusSpout }
        spouts.forEach { spout ->
            Thread {
                while (true) {
                    if (spout.isReady.get()) {
                        spout.queue.poll()?.also { ackMessage ->
                            if (ackMessage.ack) {
                                spout.ack(ackMessage.spoutMessageId)
                            } else {
                                spout.fail(ackMessage.spoutMessageId)
                            }
                        }.let {
                            if (it == null) {
                                acker.waitForSpoutAvailability()
                                if (spout.inUse.compareAndSet(false, true)) {
                                    try {
                                        spout.nextTuple()
                                    } finally {
                                        spout.inUse.set(false)
                                    }
                                }
                            }
                        }
                    }
                }
            }.apply {
                this.isDaemon = true
                this.start()
            }
        }
        started.set(true)
        if (block) {
            shutDownHook.await()
        }
    }

    fun stop() {
        println("Max pool size: ${boltExecutionPool.maximumPoolSize}")
        boltExecutionPool.shutdown()
        boltExecutionPool.awaitTermination(30, TimeUnit.SECONDS)
        shutDownHook.countDown()
    }

    // KumulusEmitter impl
    override fun getDestinations(tasks: List<Int>): List<KumulusComponent> {
        return this.components.filter { tasks.contains(it.taskId()) }
    }

    // KumulusEmitter impl
    override fun execute(destComponent: KumulusComponent, kumulusTuple: KumulusTuple) {
        mainQueue.add(ExecuteMessage(destComponent, kumulusTuple))
    }

    // KumulusEmitter impl
    override fun completeMessageProcessing(spout: KumulusSpout, spoutMessageId: Any?, ack: Boolean) {
        spout.queue.add(AckMessage(spout, spoutMessageId, ack))
    }
}