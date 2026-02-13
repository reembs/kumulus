package org.xyro.kumulus

import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.topology.KumulusTopologyBuilder
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class TestPrepareException {
    @Test(expected = KumulusTopology.KumulusTopologyCrashedException::class, timeout = 5000)
    fun testPrepareException() {
        val builder = KumulusTopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setBolt("prepare-exception-bolt", TestBolt(0))
            .noneGrouping("spout")
        val kumulusTopology =
            KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
        kumulusTopology.prepare(2, TimeUnit.SECONDS)
    }

    @Test(expected = TimeoutException::class, timeout = 10_000)
    fun testLongPrepare() {
        val builder = KumulusTopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setBolt("prepare-exception-bolt", TestBolt(30))
            .noneGrouping("spout")
        val kumulusTopology =
            KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
        kumulusTopology.prepare(2, TimeUnit.SECONDS)
    }

    class TestSpout : DummySpout({ it.declare(Fields()) }) {
        override fun nextTuple() {
            collector.emit(listOf(), Object())
        }
    }

    class TestBolt(private val prepareDelaySecs: Int) : IRichBolt {
        override fun execute(input: Tuple) = Unit
        override fun prepare(p0: MutableMap<Any?, Any?>?, p1: TopologyContext?, p2: OutputCollector) {
            Thread.sleep((prepareDelaySecs * 1000).toLong())
            throw TestException()
        }
        override fun cleanup() = Unit
        override fun getComponentConfiguration(): MutableMap<String, Any> = mutableMapOf()
        override fun declareOutputFields(p0: OutputFieldsDeclarer) = Unit
    }

    class TestException : RuntimeException("This exception should be thrown")
}
