package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.KumulusStormTransformer
import org.xyro.kumulus.KumulusTopology
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertTrue

class TestAnchoringBehavior {
    @Test
    fun testLatentUnanchoredBolt() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()
        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L
        config[KumulusTopology.CONF_BOLT_QUEUE_PUSHBACK_WAIT] = 10L

        builder.setSpout("spout", LatencyDeltaSpout())

        builder.setBolt("unanchoring-bolt", UnanchoringBolt())
                .noneGrouping("spout")

        builder.setBolt("delay-unanchored-bolt", DelayBolt())
                .noneGrouping("unanchoring-bolt")

        builder.setBolt("unanchored-bolt-2", DummyBolt())
                .noneGrouping("spout")
                .noneGrouping("delay-unanchored-bolt")

        val stormTopology = builder.createTopology()!!
        val kumulusTopology =
                KumulusStormTransformer.initializeTopology(stormTopology, config, "test")

        kumulusTopology.prepare(10, TimeUnit.SECONDS)
        kumulusTopology.start(block = false)
        Thread.sleep(5000)
        kumulusTopology.stop()

        logger.info { "Ran ${calledCount.get()} times" }
        assertTrue { calledCount.get() > 1000 }
        logger.info { "Max delay: ${maxWait.get()}ms" }
        assertTrue { maxWait.get() < 100 }
    }


    class LatencyDeltaSpout: DummySpout({
        it.declare(Fields("id"))
    }) {
        private var index: Int = 0
        private var lastCall: Long? = 0

        override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
            super.open(conf, context, collector)
            this.index = 0
            this.lastCall = null
        }

        override fun nextTuple() {
            val now = System.nanoTime()
            if (this.lastCall != null) {
                val tookMillis = TimeUnit.NANOSECONDS.toMillis(now - this.lastCall!!)
                maxWait.updateAndGet { v ->
                    tookMillis.takeIf { tookMillis > v } ?: v
                }
                if (tookMillis > 100) {
                    logger.error { "Took $tookMillis to nextTuple" }
                }
            }
            calledCount.incrementAndGet()
            this.lastCall = now
            val messageId = ++index
            collector.emit(listOf(messageId), messageId)
        }
    }

    class UnanchoringBolt: IRichBolt {
        private lateinit var collector: OutputCollector

        override fun execute(input: Tuple) {
            collector.emit(input.values)
            collector.ack(input)
        }

        override fun prepare(p0: MutableMap<Any?, Any?>?, p1: TopologyContext?, p2: OutputCollector?) {
            this.collector = p2!!
        }

        override fun cleanup() = Unit

        override fun getComponentConfiguration(): MutableMap<String, Any> {
            return mutableMapOf()
        }

        override fun declareOutputFields(p0: OutputFieldsDeclarer) {
            p0.declare(Fields("id"))
        }
    }

    class DelayBolt: DummyBolt({
        it.declare(Fields("id"))
    }) {
        override fun execute(input: Tuple, collector: BasicOutputCollector) {
            Thread.sleep(1000)
            collector.emit(input.values)
        }
    }

    companion object {
        private val logger = KotlinLogging.logger {}
        private val maxWait = AtomicLong(0)
        private val calledCount = AtomicLong(0)
    }
}