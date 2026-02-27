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

class TestUnserializableBoltField {
    @Test(expected = RuntimeException::class, timeout = 5_000)
    fun testCreateTopologyWithUnserializableBoltField() {
        val builder = KumulusTopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()
        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 2L

        builder.setSpout("spout", TestSpout())
        builder
            .setBolt("unserializable-bolt", UnserializableFieldBolt())
            .shuffleGrouping("spout")

        KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
    }

    class TestSpout : DummySpout({ it.declare(Fields()) }) {
        override fun nextTuple() = Unit
    }

    class UnserializableFieldBolt : IRichBolt {
        private val unserializable = Thread()

        override fun prepare(
            p0: MutableMap<Any?, Any?>?,
            p1: TopologyContext?,
            p2: OutputCollector?,
        ) = Unit

        override fun execute(input: Tuple?) = Unit

        override fun cleanup() = Unit

        override fun declareOutputFields(declarer: OutputFieldsDeclarer?) = Unit

        override fun getComponentConfiguration(): MutableMap<String, Any> = mutableMapOf()
    }
}
