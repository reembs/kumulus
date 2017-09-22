package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.Constants
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.collector.KumulusBoltCollector
import org.oryx.kumulus.collector.KumulusSpoutCollector
import org.oryx.kumulus.component.*
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

class KumulusTopology(
        private val components: List<KumulusComponent>,
        private val componentInputs: MutableMap<Pair<String, GlobalStreamId>, Grouping>,
        config: MutableMap<String, Any>
) : KumulusEmitter {
    private val queue: ArrayBlockingQueue<Runnable> = ArrayBlockingQueue(2000)
    private val boltExecutionPool: ThreadPoolExecutor
    private val maxSpoutPending: Int
    private val mainQueue = LinkedBlockingDeque<KumulusMessage>()
    private val acker: KumulusAcker
    private val rejectedExecutionHandler = RejectedExecutionHandler { _, _ ->
        logger.error { "Execution was rejected" }
    }
    private val tickExecutor = ScheduledThreadPoolExecutor(1, rejectedExecutionHandler)
    private val started = AtomicBoolean(false)
    private val systemComponent = components.first { it.taskId() == Constants.SYSTEM_TASK_ID.toInt() }

    init {
        boltExecutionPool = ThreadPoolExecutor(4, 10, 20, TimeUnit.SECONDS, queue)
        boltExecutionPool.prestartAllCoreThreads()
        maxSpoutPending = config[org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING] as Int? ?: 0
        acker = KumulusAcker(this, maxSpoutPending)
    }

    companion object {
        private val logger = KotlinLogging.logger {}
    }

    fun prepare() {
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
                                        null,
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

        startQueuePolling()
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
                    mainQueue.push(message)
                }
            }
        }

        Thread(pollerRunnable).apply {
            this.isDaemon = true
            this.start()
        }
    }

    fun start() {
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
    }

    fun stop() {
        println("Max pool size: ${boltExecutionPool.maximumPoolSize}")
        boltExecutionPool.shutdown()
        boltExecutionPool.awaitTermination(30, TimeUnit.SECONDS)
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