package org.xyro.kumulus

import org.apache.storm.Config
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Before
import org.junit.Test
import org.xyro.kumulus.component.KumulusTimeoutNotificationSpout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestTimeoutSpoutHook {
    @Before
    fun before() {
        done = CountDownLatch(1)
        fail = AtomicBoolean(false)
        missingBolts = AtomicReference<List<String>>(null)
        failedBolts = AtomicReference<List<String>>(null)
    }

    @Test
    fun testTopologyTimeout() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setBolt("timeout-bolt", TestBolt(true))
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
        assertTrue { missing.contains("timeout-bolt") }
        assertTrue { fail.get() }
        assertTrue { failedBolts.get().isEmpty() }
    }

    @Test
    fun testTopologyFailure() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setBolt("fail-bolt", TestFailingBolt())
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

        val failed = failedBolts.get()
        assertNotNull(failed)
        assertTrue { failed.size == 1 }
        assertTrue { failed.contains("fail-bolt") }
        assertTrue { fail.get() }
        assertTrue { missingBolts.get().isEmpty() }
    }

    class TestSpout : DummySpout({ it.declare(Fields("id")) }), KumulusTimeoutNotificationSpout {
        private var index: Int = 0

        override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
            super.open(conf, context, collector)
            this.index = 0
        }

        override fun fail(msgId: Any?) {
            fail.set(true)
            done.countDown()
        }

        override fun ack(msgId: Any?) {
            done.countDown()
        }

        override fun messageIdFailure(msgId: Any?, failedComponents: List<String>, timeoutComponents: List<String>) {
            missingBolts.set(timeoutComponents)
            failedBolts.set(failedComponents)
        }

        override fun nextTuple() {
            val messageId = ++index
            collector.emit(listOf(messageId), messageId)
        }
    }

    class TestBolt(private val shouldTimeout: Boolean) : IRichBolt {
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

    class TestFailingBolt : BaseBasicBolt() {
        override fun execute(p0: Tuple?, p1: BasicOutputCollector?) = throw FailedException()
        override fun declareOutputFields(p0: OutputFieldsDeclarer?) = Unit
    }

    companion object {
        private var done = CountDownLatch(1)
        private var fail = AtomicBoolean(false)
        private var missingBolts = AtomicReference<List<String>>(null)
        private var failedBolts = AtomicReference<List<String>>(null)
    }
}
