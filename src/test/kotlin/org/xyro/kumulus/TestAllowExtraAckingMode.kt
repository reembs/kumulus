package org.xyro.kumulus

import mu.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.component.KumulusTimeoutNotificationSpout
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertTrue

class TestAllowExtraAckingMode {
    @Test
    fun testMultipleSpouts() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 5L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L
        config[KumulusTopology.CONF_EXTRA_ACKING] = true

        builder.setSpout("spout", TestSpout())
        builder.setSpout("spout2", TestSpout())
        builder.setSpout("spout3", TestSpout())

        builder.setBolt("acking-bolt", TestBolt())
                .allGrouping("spout")
                .allGrouping("spout2")
                .allGrouping("spout3")

        builder.setBolt("acking-bolt2", TestBolt())
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

    class TestSpout: DummySpout({ it.declare(Fields("id")) }) {
        override fun fail(msgId: Any?) = Unit
        override fun ack(msgId: Any?) = Unit
        override fun nextTuple() {
            val messageId = UUID.randomUUID().toString()
            collector.emit(listOf(messageId), messageId)
        }
    }

    class TestBolt: IRichBolt {
        private lateinit var collector: OutputCollector

        override fun execute(input: Tuple) {
            executions.incrementAndGet()
            collector.ack(input)
            collector.ack(input) // extra ack
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