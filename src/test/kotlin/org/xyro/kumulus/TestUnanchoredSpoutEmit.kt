package org.xyro.kumulus

import org.apache.storm.Config
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.topology.KumulusTopologyBuilder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertTrue

class TestUnanchoredSpoutEmit {
    @Test(timeout = 5_000)
    fun testSpoutEmitWithoutMessageIdDoesNotCrashTopology() {
        TestBolt.messageSeenByBolt = CountDownLatch(1)
        TestSpout.emitted.set(false)

        val builder = KumulusTopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()
        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 2L

        builder.setSpout("spout", TestSpout())
        builder
            .setBolt("bolt", TestBolt())
            .shuffleGrouping("spout")

        val kumulusTopology =
            KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")

        kumulusTopology.prepare(5, TimeUnit.SECONDS)
        kumulusTopology.start(block = false)

        assertTrue(TestBolt.messageSeenByBolt.await(20, TimeUnit.SECONDS), "Unanchored tuple should be delivered to bolt")
        kumulusTopology.stop()
        assertTrue(TestSpout.emitted.get(), "Spout should have emitted at least one tuple")
    }

    private class TestSpout : DummySpout({ it.declare(Fields("id")) }) {
        override fun nextTuple() {
            if (emitted.compareAndSet(false, true)) {
                collector.emit(listOf(1), null)
            }
        }

        companion object {
            val emitted = AtomicBoolean(false)
        }
    }

    private class TestBolt : BaseBasicBolt() {
        override fun execute(
            input: Tuple,
            collector: BasicOutputCollector,
        ) {
            if (input.sourceComponent == "spout") {
                messageSeenByBolt.countDown()
            }
        }

        override fun declareOutputFields(declarer: OutputFieldsDeclarer) = Unit

        companion object {
            var messageSeenByBolt = CountDownLatch(1)
        }
    }
}
