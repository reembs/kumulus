package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.Config
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
    private val maxSpoutPending: Long = config[Config.TOPOLOGY_MAX_SPOUT_PENDING] as Long? ?: 0L
    private val boltExecutionPool: ThreadPoolExecutor = ThreadPoolExecutor(
            (config[CONF_THREAD_POOL_CORE_SIZE] as? Long ?: 1L).toInt(),
            (config[CONF_THREAD_POOL_MAX_SIZE] as? Long ?: components.size.toLong()).toInt(),
            config[CONF_THREAD_POOL_KEEP_ALIVE] as? Long ?: 20L,
            TimeUnit.SECONDS,
            ArrayBlockingQueue((config[CONF_THREAD_POOL_QUEUE_SIZE] as? Long)?.toInt() ?: components.size * 2),
            object: ThreadFactory {
                var count = 0
                override fun newThread(r: Runnable?): Thread {
                    count++
                    logger.info { "Creating new thread: $count" }
                    return Thread(r).apply {
                        this.isDaemon = true
                        this.name = "KumulusThread-$count"
                    }
                }
            },
            RejectedExecutionHandler { r, executor ->
                if (!executor.isShutdown && !executor.isTerminating && !executor.isTerminated) {
                    executor!!.queue.put(r)
                }
            }
    )
    private val stopLock = Any()
    private val mainQueue = LinkedBlockingQueue<KumulusMessage>()
    private val acker: KumulusAcker
    private val rejectedExecutionHandler = RejectedExecutionHandler { _, _ ->
        logger.error { "Execution was rejected" }
    }
    private val tickExecutor = ScheduledThreadPoolExecutor(1, rejectedExecutionHandler)
    private val started = AtomicBoolean(false)
    private val systemComponent = components.first { it.taskId() == Constants.SYSTEM_TASK_ID.toInt() }
    private val shutDownHook = CountDownLatch(1)
    private val busyPollSleepTime: Long = config[CONF_BUSY_POLL_SLEEP_TIME] as? Long ?: 5L
    private val shutdownTimeoutSecs = config[CONF_SHUTDOWN_TIMEOUT_SECS] as? Long ?: 10L

    var onBusyBoltHook: ((String, Int) -> Unit)? = null
    val onReportErrorHook: ((Throwable?) -> Unit)? = null

    init {
        this.boltExecutionPool.prestartAllCoreThreads()
        this.acker = KumulusAcker(
                this,
                maxSpoutPending,
                config[CONF_EXTRA_ACKING] as? Boolean ?: false,
                busyPollSleepTime
        )
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        @JvmField
        val CONF_EXTRA_ACKING = "kumulus.allow-extra-acking"
        @JvmField
        val CONF_THREAD_POOL_KEEP_ALIVE = "kumulus.thread_pool.keep_alive_secs"
        @JvmField
        val CONF_THREAD_POOL_QUEUE_SIZE = "kumulus.thread_pool.queue.size"
        @JvmField
        val CONF_THREAD_POOL_MAX_SIZE = "kumulus.thread_pool.max_size"
        @JvmField
        val CONF_THREAD_POOL_CORE_SIZE = "kumulus.thread_pool.core_pool_size"
        @JvmField
        val CONF_BUSY_POLL_SLEEP_TIME = "kumulus.spout.not-ready-sleep"
        @JvmField
        val CONF_SHUTDOWN_TIMEOUT_SECS = "kumulus.shutdown.timeout.secs"
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
                            component, KumulusSpoutCollector(component, componentRegisteredOutputs, this, acker, onReportErrorHook))
                is KumulusBolt -> {
                    BoltPrepareMessage(
                            component, KumulusBoltCollector(component, componentRegisteredOutputs, this, acker, onReportErrorHook))
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
                mainQueue.poll(1, TimeUnit.SECONDS)?.let { message ->
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
                            } catch (e: Exception) {
                                logger.error("An uncaught exception in component '${c.context.thisComponentId}' has forced a Kumulus shutdown", e)
                                this.stop()
                                throw e
                            } finally {
                                c.inUse.set(false)
                            }
                        })
                    } else {
                        logger.trace { "Component ${c.context.thisComponentId}/${c.taskId()} is currently busy" }
                        onBusyBoltHook?.let { it(c.context.thisComponentId, c.taskId()) }
                        mainQueue.add(message)
                    }
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
                try {
                    while (true) {
                        if (spout.isReady.get()) {
                            spout.activate()
                            break
                        }
                        Thread.sleep(busyPollSleepTime)
                    }
                    while (true) {
                        spout.queue.poll()?.also { ackMessage ->
                            if (ackMessage.ack) {
                                spout.ack(ackMessage.spoutMessageId)
                            } else {
                                spout.fail(ackMessage.spoutMessageId)
                            }
                        }.let {
                            if (it == null && spout.isReady.get()) {
                                acker.waitForSpoutAvailability()
                                if (spout.inUse.compareAndSet(false, true)) {
                                    try {
                                        if (spout.isReady.get()) {
                                            spout.nextTuple()
                                        }
                                    } finally {
                                        spout.inUse.set(false)
                                    }
                                }
                            }
                        }
                        if (!spout.isReady.get()) {
                            spout.deactivate()
                            return@Thread
                        }
                    }
                } catch (e: Exception) {
                    logger.error("An uncaught exception in spout '${spout.context.thisComponentId}' has forced a Kumulus shutdown", e)
                    spout.deactivate()
                    this.stop()
                    throw e
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
        synchronized(stopLock) {
            if (shutDownHook.count > 0) {
                logger.info("Max pool size: ${boltExecutionPool.largestPoolSize}")
                logger.info { "Deactivating all spouts" }
                components.filter { it is KumulusSpout }.forEach {
                    it.isReady.set(false)
                }
                acker.releaseSpoutBlocks()
                logger.info { "Shutting down thread pool and awaiting termination (max: ${shutdownTimeoutSecs}s)" }
                boltExecutionPool.shutdown()
                boltExecutionPool.awaitTermination(shutdownTimeoutSecs, TimeUnit.SECONDS)
                logger.info { "Execution engine threads have been shut down" }
                shutDownHook.countDown()
            }
        }
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