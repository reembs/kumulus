package org.oryx.kumulus

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.collector.KumulusBoltCollector
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
        private val componentToStreamToFields: MutableMap<String, Map<String, Fields>>,
        config: MutableMap<String, Any>
) : KumulusEmitter {
    private val queue: ArrayBlockingQueue<Runnable> = ArrayBlockingQueue(2000)
    private val boltExecutionPool: ThreadPoolExecutor
    private val maxSpoutPending: Int
    private val currentPending = AtomicInteger(0)
    private val random = Random()

    private val acker = KumulusAcker()

    init {
        boltExecutionPool = ThreadPoolExecutor(4, 10, 20, TimeUnit.SECONDS, queue)
        maxSpoutPending = config[org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING] as Int? ?: 2
    }

    fun prepare() {
        components.forEach { component ->
            val componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>> =
                    componentInputs
                            .filter { it.key.second._componentId == component.name() }
                            .map { Pair(it.key.second._streamId, Pair(it.key.first, it.value)) }

            component.queue.add(when (component) {
                is KumulusSpout ->
                    SpoutPrepareMessage(
                            KumulusSpoutCollector(component, componentRegisteredOutputs, this, acker))
                is KumulusBolt ->
                    BoltPrepareMessage(
                            KumulusBoltCollector(component, componentRegisteredOutputs, this, acker))

                else ->
                    throw UnsupportedOperationException()
            })
        }

        startQueuePolling()
    }

    private fun startQueuePolling() {
        val pollerRunnable = {
            while (true) {
                var empty = true
                components.forEach { c ->
                    val peekFirst = c.queue.peekFirst()
                    if (peekFirst != null && c.inUse.compareAndSet(false, true)) {
                        val message = c.queue.poll()
                        if (message == null) {
                            c.inUse.set(false)
                        } else {
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
                                            assert(!c.isSpout())
                                            (c as KumulusBolt).execute(message.tuple)
                                        }
                                    }
                                } finally {
                                    c.inUse.set(false)
                                }
                            })
                        }
                        empty = false
                    }
                }
                if (empty)
                    Thread.sleep(System.getenv("BOLT_SLEEP_TIME")?.toLong() ?: 1)
            }
        }

        for (i in 1..4) {
            val poller = Thread(pollerRunnable)
            poller.isDaemon = true
            poller.start()
        }
    }

    fun start() {
        val spouts = components.filter { it is KumulusSpout }.map { it as KumulusSpout }

        val sleepMillis: Long = System.getenv("SPOUT_SLEEP_TIME")?.toLong() ?: 10

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
        dest.queue.add(ExecuteMessage(KumulusTuple(src, streamId, tuple, anchors)))
    }
}