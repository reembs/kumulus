package org.xyro.kumulus

import mu.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertTrue

class TestMultipleSpoutsMaxPendingLimit {
    @Test
    fun testMultipleSpouts() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setSpout("spout2", TestSpout())
        builder.setSpout("spout3", TestSpout())

        builder.setBolt("acking-bolt", TestBolt())
                .allGrouping("spout")
                .allGrouping("spout2")
                .allGrouping("spout3")

        val stormTopology = builder.createTopology()!!
        val kumulusTopology =
                KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
        kumulusTopology.prepare(10, TimeUnit.SECONDS)
        kumulusTopology.start()
        Thread.sleep(5000)

        // no asserts fail

        kumulusTopology.stop()

        logger.info { "Executed ${executions.get()} times, no errors" }
        assertTrue { executions.get() > 100 }
    }

    @Test
    fun testMaxSpoutPending() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 5L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())

        builder.setBolt("sleeping-bolt", SleepingBolt(), 4)
                .shuffleGrouping("spout")

        val stormTopology = builder.createTopology()!!
        val kumulusTopology =
                KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
        kumulusTopology.prepare(10, TimeUnit.SECONDS)
        kumulusTopology.start()

        for (i in 0 until 50) {
            Thread.sleep(100)
            val actual = SleepingBolt.inFlight.get()
            assertTrue { actual <= 1 }
        }

        kumulusTopology.stop()
        logger.info { "Executed ${executions.get()} times, no errors" }
    }

    class TestSpout: DummySpout({ it.declare(Fields("id")) }) {
        private var count = 0

        override fun fail(msgId: Any?) {
        }

        override fun ack(msgId: Any?) {
        }

        override fun nextTuple() {
            val messageId = UUID.randomUUID().toString()
            collector.emit(listOf(messageId), messageId)
        }
    }

    class SleepingBolt : BaseBasicBolt() {
        override fun execute(input: Tuple, collector: BasicOutputCollector) {
            inFlight.incrementAndGet()
            Thread.sleep(100)
            inFlight.decrementAndGet()
        }
        override fun declareOutputFields(declarer: OutputFieldsDeclarer) = Unit
        companion object {
            val inFlight = AtomicInteger(0)
        }
    }

    class TestBolt: IRichBolt {
        private lateinit var collector: OutputCollector

        override fun execute(input: Tuple) {
            executions.incrementAndGet()
            collector.ack(input)
        }

        override fun prepare(p0: MutableMap<Any?, Any?>?, p1: TopologyContext?, p2: OutputCollector) {
            this.collector = p2
        }
        override fun cleanup() = Unit
        override fun getComponentConfiguration(): MutableMap<String, Any> = mutableMapOf()
        override fun declareOutputFields(p0: OutputFieldsDeclarer) = Unit
    }

    companion object {
        private val logger = KotlinLogging.logger {}
        private val executions = AtomicInteger(0)
    }
}