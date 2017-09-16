package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.collector.KumulusBoltCollector
import org.oryx.kumulus.collector.KumulusSpoutCollector
import org.oryx.kumulus.component.*
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

class KumulusTopology(
        private val components: List<KumulusComponent>,
        private val componentInputs: MutableMap<Pair<String, GlobalStreamId>, Grouping>,
        private val componentToStreamToFields: MutableMap<String, Map<String, Fields>>,
        config: MutableMap<String, Any>
) : KumulusEmitter {
    private val queue: ArrayBlockingQueue<Runnable> = ArrayBlockingQueue(2000)
    private val boltExecutionPool: ThreadPoolExecutor
    private val maxSpoutPending: Int
    private val currentPending = AtomicInteger(0)
    private val random = Random()
    private val mainQueue = LinkedBlockingDeque<KumulusMessage>()

    private val acker = KumulusAcker()

    init {
        boltExecutionPool = ThreadPoolExecutor(4, 10, 20, TimeUnit.SECONDS, queue)
        maxSpoutPending = config[org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING] as Int? ?: 2
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
                is KumulusBolt ->
                    BoltPrepareMessage(
                            component, KumulusBoltCollector(component, componentRegisteredOutputs, this, acker))

                else ->
                    throw UnsupportedOperationException()
            })
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
                            }
                        } finally {
                            c.inUse.set(false)
                        }
                    })
                } else {
                    logger.info { "Component ${c.context.thisComponentId}/${c.taskId()} is currently busy" }
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

        val sleepMillis: Long = System.getenv("SPOUT_SLEEP_TIME")?.toLong() ?: 10

        Thread {
            while (true) {
                if (currentPending.get() < maxSpoutPending) {
                    var found = false
                    spouts.forEach {
                        if (it.isReady.get() && it.inUse.compareAndSet(false, true)) {
                            found = true
                            boltExecutionPool.execute({
                                try {
                                    it.nextTuple()
                                } finally {
                                    it.inUse.set(false)
                                }
                            })
                        }
                    }
                    if (!found) {
                        Thread.sleep(sleepMillis)
                    }
                } else {
                    Thread.sleep(sleepMillis)
                }
            }
        }.also {
            it.isDaemon = true
            it.start()
        }
    }

    fun stop() {
        println("Max pool size: ${boltExecutionPool.maximumPoolSize}")
        boltExecutionPool.shutdown()
        boltExecutionPool.awaitTermination(30, TimeUnit.SECONDS)
    }

    override fun emit(
            self: KumulusComponent,
            dest: GlobalStreamId,
            grouping: Grouping,
            tuple: List<Any>,
            anchors: Collection<Tuple>?
    ): MutableList<Int> {
        val tasks = this.components.filter { it.name() == dest._componentId }
        val emitToInstance: List<KumulusComponent>

        emitToInstance =
                if (grouping.is_set_all) {
                    tasks
                } else if (grouping.is_set_none || grouping.is_set_shuffle || grouping.is_set_local_or_shuffle) {
                    listOf(tasks[Math.abs(random.nextInt() % tasks.size)])
                } else if (grouping.is_set_fields) {
                    val groupingFields = grouping._fields
                    val outputFields
                            = componentToStreamToFields[self.name()]?.get(dest._streamId)

                    var groupingHashes = 0L

                    groupingFields.forEach { gField ->
                        outputFields?.let {
                            val fieldValue = tuple[it.fieldIndex(gField)]
                            groupingHashes += fieldValue.hashCode()
                        }
                    }

                    listOf(tasks[(groupingHashes % tasks.size).toInt()])
                } else {
                    throw UnsupportedOperationException("Grouping type $grouping isn't currently supported by Kumulus")
                }

        emitToInstance.forEach {
            execute(it, self, dest._streamId, tuple, anchors)
        }

        return emitToInstance.map {
            it.taskId()
        }.toMutableList()
    }

    private fun execute(dest: KumulusComponent, src: KumulusComponent, streamId: String, tuple: List<Any>, anchors: Collection<Tuple>?) {
        mainQueue.add(ExecuteMessage(dest, KumulusTuple(src, streamId, tuple, anchors)))
    }
}