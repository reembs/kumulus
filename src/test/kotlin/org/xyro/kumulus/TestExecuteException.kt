package org.xyro.kumulus

import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.KumulusStormTransformer
import org.xyro.kumulus.KumulusTopology
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class TestExecuteException {
    @Test(expected = KumulusTopology.KumulusTopologyCrashedException::class, timeout = 5000)
    fun testBoltExecuteException() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 10L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setBolt("execute-exception-bolt", TestExecuteExceptionBolt())
                .noneGrouping("spout")

        val stormTopology = builder.createTopology()!!
        val kumulusTopology =
                KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
        kumulusTopology.prepare(2, TimeUnit.SECONDS)
        kumulusTopology.start(true)
    }

    @Test(expected = KumulusTopology.KumulusTopologyCrashedException::class, timeout = 5000)
    fun testSpoutNextTupleException() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 10L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TesExceptiontSpout())

        val stormTopology = builder.createTopology()!!
        val kumulusTopology =
                KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
        kumulusTopology.prepare(2, TimeUnit.SECONDS)
        kumulusTopology.start(true)
    }

    class TestSpout: DummySpout({ it.declare(Fields()) }) {
        override fun nextTuple() {
            collector.emit(listOf(), Object())
        }
    }

    class TesExceptiontSpout: DummySpout({ it.declare(Fields()) }) {
        override fun nextTuple() {
            throw RuntimeException("This exception should be thrown")
        }
    }

    class TestExecuteExceptionBolt : IRichBolt {
        override fun execute(input: Tuple) = throw RuntimeException("This exception should be thrown")
        override fun prepare(p0: MutableMap<Any?, Any?>?, p1: TopologyContext?, p2: OutputCollector) {}
        override fun cleanup() = Unit
        override fun getComponentConfiguration(): MutableMap<String, Any> = mutableMapOf()
        override fun declareOutputFields(p0: OutputFieldsDeclarer) = Unit
    }

    class TestException : RuntimeException("This exception should be thrown")
}