package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.KumulusStormTransformer
import org.xyro.kumulus.KumulusTopology
import org.xyro.kumulus.component.KumulusTimeoutAwareSpout
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestTimeoutSpoutHookMultipleAcking {
    @Test
    fun testMultipleSpouts() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 4L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setSpout("spout2", TestSpout())
        builder.setSpout("spout3", TestSpout())

        builder.setBolt("acking-bolt", TestBolt(false))
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
    }

    class TestSpout: DummySpout({ it.declare(Fields("id")) }), KumulusTimeoutAwareSpout {
        override fun fail(msgId: Any?) {
        }

        override fun ack(msgId: Any?) {
        }

        override fun fail(msgId: Any?, timeoutComponents: List<String>) {
        }

        override fun nextTuple() {
            val messageId = UUID.randomUUID().toString()
            collector.emit(listOf(messageId), messageId)
        }
    }

    class TestBolt(private  val shouldTimeout: Boolean): IRichBolt {
        private lateinit var collector: OutputCollector

        override fun execute(input: Tuple) {
            if (!shouldTimeout) {
                logger.info { "Acking ${input.getValueByField("id")}" }
                collector.ack(input)
            }
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
    }
}