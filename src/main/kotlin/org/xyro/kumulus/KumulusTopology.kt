package org.xyro.kumulus

import mu.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.Constants
import org.xyro.kumulus.collector.KumulusBoltCollector
import org.xyro.kumulus.collector.KumulusSpoutCollector
import org.xyro.kumulus.component.*
import org.xyro.kumulus.graph.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class KumulusTopology(
        private val components: List<KumulusComponent>,
        config: Map<String, Any>
) : KumulusEmitter {
    private val maxSpoutPending: Long = config[Config.TOPOLOGY_MAX_SPOUT_PENDING] as Long? ?: 0L
    private val poolSize = (config[CONF_THREAD_POOL_CORE_SIZE] as? Long ?: 1L).toInt()
    private val boltExecutionPool = ExecutionPool(poolSize, ::handleQueueItem)
    private val stopLock = Any()
    private val rejectedExecutionHandler = RejectedExecutionHandler { _, _ ->
        logger.error { "Execution was rejected" }
    }
    private val tickExecutor = ScheduledThreadPoolExecutor(1, rejectedExecutionHandler)
    private val started = AtomicBoolean(false)
    private val systemComponent = components.first { it.taskId == Constants.SYSTEM_TASK_ID.toInt() }
    private val shutDownHook = CountDownLatch(1)
    private val shutdownTimeoutSecs = config[CONF_SHUTDOWN_TIMEOUT_SECS] as? Long ?: 10L
    private val taskIdToComponent: Map<Int, KumulusComponent> = this.components
            .map { Pair(it.taskId, it) }
            .toMap()
    private val atomicThreadsInUse = AtomicInteger(0)
    private val atomicMaxThreadsInUse = AtomicInteger(0)

    internal val acker: KumulusAcker
    internal val busyPollSleepTime: Long = config[CONF_BUSY_POLL_SLEEP_TIME] as? Long ?: 1L

    var onBusyBoltHook: ((String, Int, Long, Any?) -> Unit)? = null
    var onBoltPrepareFinishHook: ((String, Int, Long) -> Unit)? = null
    var onReportErrorHook: ((String, Int, Throwable) -> Unit)? = null

    val currentThreadsInUse: Int
        get() = atomicThreadsInUse.get()

    val maxThreadsInUse: Int
        get() = atomicMaxThreadsInUse.get()

    val maxQueueSize: Int
        get() = boltExecutionPool.maxSize.get()

    init {
        this.acker = KumulusAcker(
                this,
                maxSpoutPending,
                config[CONF_EXTRA_ACKING] as? Boolean ?: false,
                (config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] as? Long)?.times(1000) ?: 0L
        )
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        @JvmField
        val CONF_EXTRA_ACKING = "kumulus.allow-extra-acking"
        @JvmField
        @Deprecated("Not in use anymore")
        val CONF_THREAD_POOL_KEEP_ALIVE = "kumulus.thread_pool.keep_alive_secs"
        @JvmField
        @Deprecated("Not in use anymore")
        val CONF_THREAD_POOL_QUEUE_SIZE = "kumulus.thread_pool.queue.size"
        @JvmField
        @Deprecated("Not in use anymore")
        val CONF_THREAD_POOL_MAX_SIZE = "kumulus.thread_pool.max_size"
        @JvmField
        val CONF_THREAD_POOL_CORE_SIZE = "kumulus.thread_pool.core_pool_size"
        @JvmField
        val CONF_BUSY_POLL_SLEEP_TIME = "kumulus.spout.not-ready-sleep"
        @JvmField
        val CONF_SHUTDOWN_TIMEOUT_SECS = "kumulus.shutdown.timeout.secs"
    }

    /**
     * Do the prepare phase of the topology
     * @param time timeout duration
     * @param unit timeout duration unit
     */
    @Throws(TimeoutException::class)
    fun prepare(time: Long, unit: TimeUnit) {
        val start = System.currentTimeMillis()

        this.prepare()

        val allReady = { this.components.all { it.isReady.get() } }

        while (System.currentTimeMillis() < start + unit.toMillis(time)) {
            if (allReady()) {
                break
            }
            Thread.sleep(busyPollSleepTime)
        }

        if (!allReady()) {
            throw TimeoutException()
        }
    }

    /**
     * Do the prepare phase of the topology
     */
    private fun prepare() {
        components.forEach { component ->
            boltExecutionPool.enqueue(when (component) {
                is KumulusSpout ->
                    SpoutPrepareMessage(
                            component, KumulusSpoutCollector(component, this, acker, onReportErrorHook))
                is KumulusBolt -> {
                    BoltPrepareMessage(
                            component, KumulusBoltCollector(component, this, acker, onReportErrorHook))
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
                                boltExecutionPool.enqueue(ExecuteMessage(component, tuple))
                            } catch (e: Exception) {
                                logger.error(e) { "Error in sending tick tuple" }
                            }
                        }
                    }, tickSecs, tickSecs, TimeUnit.SECONDS)
                }
            }
        }
    }

    /**
     * Start the topology spout polling
     * @param block should the call block until the topology os stopped
     */
    fun start(block: Boolean = false) {
        this.resetMetrics()
        val spouts = components.filter { it is KumulusSpout }.map { it as KumulusSpout }
        spouts.forEach { spout ->
            spout.start(this)
        }
        started.set(true)
        if (block) {
            shutDownHook.await()
        }
    }

    /**
     * Stop the topology
     */
    fun stop() {
        synchronized(stopLock) {
            if (shutDownHook.count > 0) {
                logger.info("Pool size: $poolSize")
                logger.info("Max queue size: $maxQueueSize")
                logger.info("Max concurrent threads used: $maxThreadsInUse")
                logger.info { "Deactivating all spouts" }
                components.filter { it is KumulusSpout }.forEach {
                    it.isReady.set(false)
                }
                acker.releaseSpoutBlocks()
                logger.info { "Shutting down thread pool and awaiting termination (max: ${shutdownTimeoutSecs}s)" }
                logger.info { "Execution engine threads have been shut down" }
                shutDownHook.countDown()
            }
        }
    }

    // KumulusEmitter impl
    override fun getDestinations(tasks: List<Int>): List<KumulusComponent> {
        return tasks.map { taskIdToComponent[it]!! }
    }

    // KumulusEmitter impl
    override fun execute(destComponent: KumulusComponent, kumulusTuple: KumulusTuple) {
        boltExecutionPool.enqueue(ExecuteMessage(destComponent, kumulusTuple))
    }

    // KumulusEmitter impl
    override fun completeMessageProcessing(
            spout: KumulusSpout,
            spoutMessageId: Any?,
            ack: Boolean,
            timeoutTasks: List<Int>
    ) {
        val timeoutComponents = timeoutTasks.map { this.taskIdToComponent[it]!!.componentId }
        spout.queue.add(AckMessage(spout, spoutMessageId, ack, timeoutComponents))
    }

    fun getGraph() : ComponentGraph<GraphNode, GraphEdge<GraphNode>> {
        return getGraph(defaultNodeFactory, defaultEdgeFactory)
    }

    fun <N: GraphNode, E: GraphEdge<N>> getGraph(
            nodeFactory : ComponentGraphNodeFactory<N>,
            edgeFactory : ComponentGraphEdgeFactory<N, E>
    ) : ComponentGraph<GraphNode, GraphEdge<GraphNode>> {
        return ComponentGraph(this.components, nodeFactory, edgeFactory)
    }

    private fun handleQueueItem(message: KumulusMessage) {
        val c = message.component
        if (c.inUse.compareAndSet(false, true)) {
            val currentThreadsInAction = atomicThreadsInUse.incrementAndGet()
            if (currentThreadsInAction > atomicMaxThreadsInUse.get()) {
                atomicMaxThreadsInUse.updateAndGet { currentMax ->
                    currentThreadsInAction.takeIf { it > currentMax } ?: currentMax
                }
            }
            try {
                when (message) {
                    is PrepareMessage<*> -> {
                        c.prepareStart.set(System.nanoTime())
                        try {
                            if (c.isSpout())
                                (c as KumulusSpout).prepare(message.collector as KumulusSpoutCollector)
                            else
                                (c as KumulusBolt).prepare(message.collector as KumulusBoltCollector)
                        } finally {
                            onBoltPrepareFinishHook?.let {
                                it(c.componentId, c.taskId,System.nanoTime() - c.prepareStart.get())
                            }
                        }
                    }
                    is ExecuteMessage -> {
                        assert(!c.isSpout()) {
                            logger.error {
                                "Execute message got to a spout '${c.componentId}', " +
                                        "this shouldn't happen."
                            }
                        }
                        onBusyBoltHook?.let {
                            val waitNanos = c.waitStart.getAndSet(0)
                            if (waitNanos > 0) {
                                it(
                                        c.componentId,
                                        c.taskId,
                                        System.nanoTime() - waitNanos,
                                        message.tuple.spoutMessageId
                                )
                            }
                        }
                        (c as KumulusBolt).execute(message.tuple)
                    }
                    else ->
                        throw UnsupportedOperationException("Operation of type ${c.javaClass.canonicalName} is unsupported")
                }
            } catch (e: Exception) {
                logger.error("An uncaught exception in component '${c.componentId}' has forced a Kumulus shutdown", e)
                this.stop()
                throw e
            } finally {
                c.inUse.set(false)
                atomicThreadsInUse.decrementAndGet()
            }
        } else {
            logger.trace { "Component ${c.componentId}/${c.taskId} is currently busy" }
            if (onBusyBoltHook != null && message is ExecuteMessage) {
                c.waitStart.compareAndSet(0, System.nanoTime())
            }
            boltExecutionPool.enqueue(message)
        }
    }

    fun resetMetrics() {
        this.atomicMaxThreadsInUse.set(0)
        this.boltExecutionPool.maxSize.set(0)
    }
}

