package org.oryx.kumulus

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.tuple.Fields
import org.oryx.kumulus.collector.KumulusBoltCollector
import org.oryx.kumulus.collector.KumulusCollector
import org.oryx.kumulus.collector.KumulusSpoutCollector
import org.oryx.kumulus.component.*
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class KumulusTopology(
        private val components: List<KumulusComponent>,
        private val componentInputs: MutableMap<Pair<String, GlobalStreamId>, Grouping>,
        private val componentToStreamToFields : MutableMap<String, Map<String, Fields>>,
        config: MutableMap<String, Any>
) : KumulusEmitter {
    private val queue: ArrayBlockingQueue<Runnable> = ArrayBlockingQueue(2000)
    private val spoutExecutionPool: ThreadPoolExecutor
    private val boltExecutionPool: ThreadPoolExecutor
    private val maxSpoutPending : Int
    private val currentPending = AtomicInteger(0)
    private val random = Random()

    private val acker = KumulusAcker()

    init {
        spoutExecutionPool = ThreadPoolExecutor(4, 10, 20, TimeUnit.SECONDS, queue)
        boltExecutionPool = ThreadPoolExecutor(4, 10, 20, TimeUnit.SECONDS, queue)
        maxSpoutPending = config[org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING] as Int? ?: 2
    }

    fun prepare() {
        startQueuePolling()

        components.forEach { component ->
            val componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>> =
                    componentInputs
                            .filter { it.key.second._componentId == component.name() }
                            .map { Pair(it.key.second._streamId, Pair(it.key.first, it.value)) }

            val collector : KumulusCollector = when (component) {
                is KumulusSpout ->
                    KumulusSpoutCollector(component, componentRegisteredOutputs, this, acker)
                is KumulusBolt ->
                    KumulusBoltCollector(component, componentRegisteredOutputs, this, acker)
                else -> {
                    throw UnsupportedOperationException()
                }
            }

            component.queue.add(PrepareMessage(collector))
        }
    }

    private fun startQueuePolling() {
        val poller = Thread({
            while (true) {
                components.forEach { c ->
                    val peekFirst = c.queue.peekFirst()
                    if (peekFirst != null && c.inUse.compareAndSet(false, true)) {
                        val message = c.queue.pop() // single thread so no races


                    }
                }
                Thread.sleep(100)
            }
        })

        poller.isDaemon = true
        poller.start()

    }

    fun start() {
        val spouts = components.filter { it is KumulusSpout } .map { it as KumulusSpout }
        val bolts = components.filter { it !is KumulusSpout } .map { it as KumulusBolt }

        val sleepMillis: Long = 20

        while (true) {
            if (currentPending.get() < maxSpoutPending) {
                var found = false
                spouts.forEach {
                    if (it.inUse.compareAndSet(false, true)) {
                        found = true
                        spoutExecutionPool.execute({
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
    }

    fun stop() {
        println("Max pool size: ${spoutExecutionPool.maximumPoolSize}")
        spoutExecutionPool.shutdown()
        spoutExecutionPool.awaitTermination(30, TimeUnit.SECONDS)
    }

    override fun emit(
            self: KumulusComponent,
            dest: GlobalStreamId,
            grouping: Grouping,
            tuple: MutableList<Any>?
    ) : MutableList<Int> {
        val tasks = this.components.filter { it.name() == dest._componentId }
        val emitToInstance : List<KumulusComponent>

        emitToInstance =
                if (grouping.is_set_all) {
                    tasks
                } else if (grouping.is_set_none || grouping.is_set_shuffle || grouping.is_set_local_or_shuffle) {
                    listOf(tasks[random.nextInt() % tasks.size])
                } else if (grouping.is_set_fields) {
                    val groupingFields = grouping._fields
                    val outputFields
                            = componentToStreamToFields[self.name()]?.get(dest._streamId)

                    var groupingHashes = 0L

                    groupingFields.forEach { gField ->
                        val fieldValue = tuple?.get(outputFields?.fieldIndex(gField)!!)
                        groupingHashes += fieldValue?.hashCode() ?: 0
                    }

                    listOf(tasks[(groupingHashes % tasks.size).toInt()])
                } else {
                    throw UnsupportedOperationException("Grouping type $grouping isn't currently supported by Kumulus")
                }

        emitToInstance.forEach {
            execute(it, self, dest._streamId, tuple!!)
        }

        return emitToInstance.map {
            it.taskId()
        }.toMutableList()
    }

    private fun execute(dest: KumulusComponent, src: KumulusComponent, streamId: String, tuple: MutableList<Any>) {
        dest.queue.add(ExecuteMessage(KumulusTuple(src, streamId, tuple)))
    }
}