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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestTimeoutSpoutHook {
    @Test
    fun testMissingTopologyTimeout() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setBolt("failing-bolt", TestBolt(true))
                .noneGrouping("spout")
        builder.setBolt("acking-bolt", TestBolt(false))
                .noneGrouping("spout")

        val stormTopology = builder.createTopology()!!
        val kumulusTopology =
                KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
        kumulusTopology.prepare(10, TimeUnit.SECONDS)
        kumulusTopology.start(block = false)
        done.await()
        kumulusTopology.stop()

        val missing = missingBolts.get()
        assertNotNull(missing)
        assertTrue { missing.size == 1 }
        assertTrue { missing.contains("failing-bolt") }
        assertFalse { fail.get() }
    }

    class TestSpout: DummySpout({ it.declare(Fields("id")) }), KumulusTimeoutAwareSpout {
        private var index: Int = 0

        override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
            super.open(conf, context, collector)
            this.index = 0
            started.set(System.currentTimeMillis())
        }

        override fun fail(msgId: Any?) {
            fail.set(true)
            done.countDown()
        }

        override fun ack(msgId: Any?) {
            done.countDown()
        }

        override fun fail(msgId: Any?, timeoutComponents: List<String>) {
            missingBolts.set(timeoutComponents)
            done.countDown()
        }

        override fun nextTuple() {
            val messageId = ++index
            collector.emit(listOf(messageId), messageId)
        }
    }

    class TestBolt(private  val shouldTimeout: Boolean): IRichBolt {
        private lateinit var collector: OutputCollector

        override fun execute(input: Tuple) {
            if (!shouldTimeout) {
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
        private val done = CountDownLatch(1)
        private val started = AtomicLong(0)
        private val fail = AtomicBoolean(false)
        private val missingBolts = AtomicReference<List<String>>(null)
    }
}