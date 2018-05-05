package org.xyro.kumulus

import mu.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.Constants
import org.apache.storm.generated.StreamInfo
import org.apache.storm.tuple.Fields
import org.xyro.kumulus.collector.KumulusBoltCollector
import org.xyro.kumulus.collector.KumulusSpoutCollector
import org.xyro.kumulus.component.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

class KumulusTopology(
        private val components: List<KumulusComponent>,
        private val config: Map<String, Any>,
        private val componentDataMap: Map<Int, ComponentData>,
        private val streams: MutableMap<String, MutableMap<String, StreamInfo>>,
        private val stormId: String = "topology"
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
    //private val systemComponent = components.first { it.taskId == Constants.SYSTEM_TASK_ID.toInt() }
    private val shutDownHook = CountDownLatch(1)
    private val shutdownTimeoutSecs = config[CONF_SHUTDOWN_TIMEOUT_SECS] as? Long ?: 10L
    private val taskToComponent = components.map { it.taskId to it }.toMap()

    internal val acker: KumulusAcker
    internal val busyPollSleepTime: Long = config[CONF_BUSY_POLL_SLEEP_TIME] as? Long ?: 1L

    var onBusyBoltHook: ((String, Int, Long, Any?) -> Unit)? = null
    var onBoltPrepareFinishHook: ((String, Int, Long) -> Unit)? = null
    var onReportErrorHook: ((String, Int, Throwable) -> Unit)? = null

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

        const val CONF_EXTRA_ACKING = "kumulus.allow-extra-acking"
        @Suppress("unused")
        @Deprecated("Not in use anymore")
        const val CONF_THREAD_POOL_KEEP_ALIVE = "kumulus.thread_pool.keep_alive_secs"
        @Suppress("unused")
        @Deprecated("Not in use anymore")
        const val CONF_THREAD_POOL_QUEUE_SIZE = "kumulus.thread_pool.queue.size"
        @Suppress("unused")
        @Deprecated("Not in use anymore")
        const val CONF_THREAD_POOL_MAX_SIZE = "kumulus.thread_pool.max_size"
        const val CONF_THREAD_POOL_CORE_SIZE = "kumulus.thread_pool.core_pool_size"
        const val CONF_BUSY_POLL_SLEEP_TIME = "kumulus.spout.not-ready-sleep"
        const val CONF_SHUTDOWN_TIMEOUT_SECS = "kumulus.shutdown.timeout.secs"
    }

    fun validateTopology() {
        components.forEach { src ->
            (src as? KumulusBolt)?.apply {
                inputs.forEach { gid, grouping ->
                    val input = components.find { it.componentId == gid._componentId } ?:
                            throw KumulusStormTransformer.KumulusTopologyValidationException(
                                    "Component '$componentId' is connected to non-existent component " +
                                            "'${gid._componentId}'")

                    when (input) {
                        is KumulusBolt -> {
                            if (!input.outputs.containsKey(gid._streamId)) {
                                throw KumulusStormTransformer.KumulusTopologyValidationException(
                                        "Component '$componentId' is connected to non-existent stream " +
                                                "'${gid._streamId}' of component '${gid._componentId}'")
                            }
                            if (grouping.is_set_fields) {
                                val declaredFields = input.outputs[gid._streamId]!!._output_fields.toSet()
                                if (!grouping._fields.all { declaredFields.contains(it) }) {
                                    throw KumulusStormTransformer.KumulusTopologyValidationException(
                                            "Component '$componentId' is connected to stream '${gid._streamId}' of component " +
                                                    "'${gid._componentId}' grouped by non existing fields ${grouping._fields}")
                                }
                            }
                        }
                        is KumulusSpout -> {}
                        else -> throw Exception("Unexpected error")
                    }
                }
            }
        }
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
        val componentToStreamToFields: Map<String, Map<String, Fields>> =
                streams.map { (k, v) ->
                    val streamMap = v.map { (stream, info) ->
                        stream to Fields(info._output_fields)
                    }.toMap()
                    k to streamMap
                }.toMap()

        val taskToComponentName: Map<Int, String> =
                components.map { it.taskId to it.componentId }.toMap()

        val componentToSortedTasks: Map<String, List<Int>> =
                components
                        .map { it.componentId to it.taskId }
                        .groupBy { (componentName) -> componentName }
                        .mapValues { it.value.map { (_, task) -> task } }


        components.forEach { component ->
            val componentData = componentDataMap[component.taskId]!!

            val rawContext = componentData.createContext(
                    config,
                    taskToComponentName,
                    componentToSortedTasks,
                    componentToStreamToFields,
                    stormId,
                    component.taskId)

            boltExecutionPool.enqueue(when (component) {
                is KumulusSpout ->
                    SpoutPrepareMessage(
                            component,
                            rawContext,
                            KumulusSpoutCollector(component, this, acker, onReportErrorHook)
                    )
                is KumulusBolt -> {
                    BoltPrepareMessage(
                            component,
                            rawContext,
                            KumulusBoltCollector(component, this, acker, onReportErrorHook)
                    )
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
                                        taskToComponent[Constants.SYSTEM_TASK_ID.toInt()]!!,
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
                logger.info("Max queue size: ${boltExecutionPool.maxSize.get()}")
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
        return tasks.map { taskToComponent[it]!! }
    }

    // KumulusEmitter impl
    override fun execute(destComponent: KumulusComponent, kumulusTuple: KumulusTuple) {
        boltExecutionPool.enqueue(ExecuteMessage(destComponent, kumulusTuple))
    }

    // KumulusEmitter impl
    override fun completeMessageProcessing(spout: KumulusSpout, spoutMessageId: Any?, ack: Boolean) {
        spout.queue.add(AckMessage(spout, spoutMessageId, ack))
    }

    private fun handleQueueItem(message: KumulusMessage) {
        val c = message.component
        if (c.inUse.compareAndSet(false, true)) {
            try {
                when (message) {
                    is PrepareMessage<*> -> {
                        c.setContext(message.context)
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
            }
        } else {
            logger.error { "Component ${c.componentId}/${c.taskId} is currently busy" }
            if (onBusyBoltHook != null && message is ExecuteMessage) {
                c.waitStart.compareAndSet(0, System.nanoTime())
            }
            boltExecutionPool.enqueue(message)
        }
    }
}
