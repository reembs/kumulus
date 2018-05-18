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
import java.util.concurrent.atomic.AtomicReference

class KumulusTopology(
        private val components: List<KumulusComponent>,
        config: Map<String, Any>
) : KumulusEmitter {
    private val maxSpoutPending: Long = config[Config.TOPOLOGY_MAX_SPOUT_PENDING] as Long? ?: 0L
    private val poolSize = (config[CONF_THREAD_POOL_CORE_SIZE] as? Long ?: 1L).toInt()
    private val boltExecutionPool = ExecutionPool(poolSize, ::handleQueueItem)
    private val stopLock = Any()
    private val started = AtomicBoolean(false)
    private val systemComponent = components.first { it.taskId == Constants.SYSTEM_TASK_ID.toInt() }
    private val shutDownHook = CountDownLatch(1)
    private val crashException = AtomicReference<Throwable>()
    private val shutdownTimeoutSecs = config[CONF_SHUTDOWN_TIMEOUT_SECS] as? Long ?: 10L
    private val taskIdToComponent: Map<Int, KumulusComponent> = this.components
            .map { Pair(it.taskId, it) }
            .toMap()
    private val atomicThreadsInUse = AtomicInteger(0)
    private val atomicMaxThreadsInUse = AtomicInteger(0)

    private val scheduledExecutorPoolSize: Int =
            (config[CONF_SCHEDULED_EXECUTOR_THREAD_POOL_SIZE] as? Long ?: 1L).toInt()
    private val rejectedExecutionHandler = RejectedExecutionHandler { _, _ ->
        logger.error { "Execution was rejected, current pool size: $scheduledExecutorPoolSize" }
    }
    private val scheduledExecutor = ScheduledThreadPoolExecutor(scheduledExecutorPoolSize, rejectedExecutionHandler)

    internal val acker: KumulusAcker
    internal val readyPollSleepTime: Long = config[CONF_READY_POLL_SLEEP] as? Long ?: 100L
    internal val queuePushbackWait: Long = config[CONF_BOLT_QUEUE_PUSHBACK_WAIT] as? Long ?: 0L

    var onBusyBoltHook: ((String, Int, Long, Any?) -> Unit)? = null
    var onBoltPrepareFinishHook: ((String, Int, Long) -> Unit)? = null
    var onReportErrorHook: ((String, Int, Throwable) -> Unit)? = null

    @Suppress("MemberVisibilityCanBePrivate", "unused")
    val currentThreadsInUse: Int
        get() = atomicThreadsInUse.get()

    @Suppress("MemberVisibilityCanBePrivate")
    val maxThreadsInUse: Int
        get() = atomicMaxThreadsInUse.get()

    @Suppress("MemberVisibilityCanBePrivate")
    val maxQueueSize: Int
        get() = boltExecutionPool.maxSize.get()

    init {
        this.acker = KumulusAcker(
                this,
                maxSpoutPending,
                config[CONF_EXTRA_ACKING] as? Boolean ?: false,
                (config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] as? Long)?.times(1000) ?: 0L,
                config[CONF_SPOUT_AVAILABILITY_PASS_TIMEOUT] as? Long ?: 50L
        )
    }

    companion object {
        private val logger = KotlinLogging.logger {}
        const val CONF_EXTRA_ACKING = "kumulus.allow-extra-acking"
        const val CONF_THREAD_POOL_CORE_SIZE = "kumulus.thread_pool.core_pool_size"
        const val CONF_READY_POLL_SLEEP = "kumulus.spout.ready.poll.sleep"
        const val CONF_SPOUT_AVAILABILITY_PASS_TIMEOUT = "kumulus.spout.availability.timeout"
        const val CONF_BOLT_QUEUE_PUSHBACK_WAIT = "kumulus.bolt.pushback.wait"
        const val CONF_SHUTDOWN_TIMEOUT_SECS = "kumulus.shutdown.timeout.secs"
        const val CONF_SCHEDULED_EXECUTOR_THREAD_POOL_SIZE = "kumulus.executor.scheduled-executor.pool-size"

        @Deprecated("Use CONF_READY_POLL_SLEEP instead")
        const val CONF_BUSY_POLL_SLEEP_TIME = CONF_READY_POLL_SLEEP
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
            Thread.sleep(readyPollSleepTime)
            throwIfNeeded()
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
                    scheduledExecutor.scheduleWithFixedDelay({
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
            throwIfNeeded()
        }
    }

    /**
     * Stop the topology
     */
    fun stop() {
        stopInternal()
        throwIfNeeded()
    }

    private fun throwIfNeeded() {
        val exception = crashException.getAndSet(null)
        if (exception != null) {
            throw KumulusTopologyCrashedException(exception)
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

    // KumulusEmitter impl
    override fun throwException(t: Throwable) {
        logger.error("Exception from emitter: $t", t)
        crashException.compareAndSet(null, t)
        this.stopInternal()
    }

    fun getGraph() : ComponentGraph<GraphNode, GraphEdge<GraphNode>> {
        return getGraph(defaultNodeFactory, defaultEdgeFactory)
    }

    @Suppress("MemberVisibilityCanBePrivate")
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
                        if(c.isSpout()) {
                            throw RuntimeException("Execute message got to a spout '${c.componentId}', this shouldn't happen.")
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
                logger.error("An uncaught exception in component '${c.componentId}' had forced a Kumulus shutdown", e)
                throwException(e)
            } finally {
                c.inUse.set(false)
                atomicThreadsInUse.decrementAndGet()
            }
        } else {
            logger.trace { "Component ${c.componentId}/${c.taskId} is currently busy" }
            if (onBusyBoltHook != null && message is ExecuteMessage) {
                c.waitStart.compareAndSet(0, System.nanoTime())
            }
            if (queuePushbackWait <= 0L) {
                boltExecutionPool.enqueue(message)
            } else {
                scheduledExecutor.schedule({
                    boltExecutionPool.enqueue(message)
                }, queuePushbackWait, TimeUnit.MILLISECONDS)
            }
        }
    }

    @Suppress("MemberVisibilityCanBePrivate")
    fun resetMetrics() {
        this.atomicMaxThreadsInUse.set(0)
        this.boltExecutionPool.maxSize.set(0)
    }

    private fun stopInternal() {
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

    class KumulusTopologyCrashedException(exception: Throwable?) : RuntimeException(
            "Kumulus topology had crashed due to an uncaught exception",
            exception
    )
}

