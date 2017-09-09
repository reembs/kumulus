package org.oryx.kumulus

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.oryx.kumulus.collector.KumulusBoltCollector
import org.oryx.kumulus.collector.KumulusSpoutCollector
import org.oryx.kumulus.component.KumulusBolt
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class KumulusTopology(
        private val components: List<KumulusComponent>,
        private val componentInputs: MutableMap<Pair<String, GlobalStreamId>, Grouping>,
        config: MutableMap<String, Any>
) : KumulusEmitter {
    private val queue: ArrayBlockingQueue<Runnable> = ArrayBlockingQueue(2000)
    private val ex : ThreadPoolExecutor
    private val maxSpoutPending : Int
    private val currentPending = AtomicInteger(0)

    init {
        ex = ThreadPoolExecutor(4, 10, 20, TimeUnit.SECONDS, queue)
        ex.prestartAllCoreThreads()

        maxSpoutPending = config[org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING] as Int? ?: 2
    }

    fun prepare() {
        components.forEach { component ->
            val componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>> =
                    componentInputs
                            .filter { it.key.second._componentId == component.name() }
                            .map { Pair(it.key.second._streamId, Pair(it.key.first, it.value)) }

            val componentOutputsMap = componentRegisteredOutputs.toMap()

            ex.execute({
                when (component) {
                    is KumulusSpout -> {
                        component.prepare(KumulusSpoutCollector(componentOutputsMap, this))
                    }
                    is KumulusBolt -> {
                        component.prepare(KumulusBoltCollector(componentOutputsMap, this))
                    }
                }
            })
        }
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
                        ex.execute({
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
        println("Max pool size: ${ex.maximumPoolSize}")
        ex.shutdown()
        ex.awaitTermination(30, TimeUnit.SECONDS)
    }

    override fun emit(dest: GlobalStreamId, grouping: Grouping, tuple: MutableList<Any>?) : MutableList<Int> {
        return mutableListOf()
    }
}