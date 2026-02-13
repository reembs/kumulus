package org.xyro.kumulus

import org.apache.storm.Config
import org.apache.storm.shade.org.json.simple.JSONValue
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.IRichSpout
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.topology.KumulusTopologyBuilder
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class KumulusTopologyBuilderConfigurationParityTest {
    @Test
    fun testSpoutAddConfigurationParityWithStorm() {
        val componentConfig = mutableMapOf<String, Any?>("existing" to "value", "n" to 1)

        val stormBuilder = TopologyBuilder()
        val stormDeclarer = stormBuilder.setSpout("spout", ConfigurableSpout(componentConfig))
        stormDeclarer.addConfiguration("k1", "v1")
        stormDeclarer.addConfiguration("k2", 2)
        stormDeclarer.addConfigurations(mapOf("k3" to true, "k1" to "override"))
        val stormJson = stormBuilder.createTopology()._spouts["spout"]!!._common._json_conf

        val nativeBuilder = KumulusTopologyBuilder()
        val nativeDeclarer = nativeBuilder.setSpout("spout", ConfigurableSpout(componentConfig))
        nativeDeclarer.addConfiguration("k1", "v1")
        nativeDeclarer.addConfiguration("k2", 2)
        nativeDeclarer.addConfigurations(mapOf("k3" to true, "k1" to "override"))
        val nativeJson = nativeBuilder.createTopology().spouts["spout"]!!.common._json_conf

        assertEquals(parseJson(stormJson), parseJson(nativeJson))
    }

    @Test
    fun testBoltAddConfigurationParityWithStorm() {
        val componentConfig = mutableMapOf<String, Any?>("existing" to "value", "n" to 1)

        val stormBuilder = TopologyBuilder()
        val stormDeclarer = stormBuilder.setBolt("bolt", ConfigurableBolt(componentConfig))
        stormDeclarer.addConfiguration("k1", "v1")
        stormDeclarer.addConfiguration("k2", 2)
        stormDeclarer.addConfigurations(mapOf("k3" to true, "k1" to "override"))
        val stormJson = stormBuilder.createTopology()._bolts["bolt"]!!._common._json_conf

        val nativeBuilder = KumulusTopologyBuilder()
        val nativeDeclarer = nativeBuilder.setBolt("bolt", ConfigurableBolt(componentConfig))
        nativeDeclarer.addConfiguration("k1", "v1")
        nativeDeclarer.addConfiguration("k2", 2)
        nativeDeclarer.addConfigurations(mapOf("k3" to true, "k1" to "override"))
        val nativeJson = nativeBuilder.createTopology().bolts["bolt"]!!.common._json_conf

        assertEquals(parseJson(stormJson), parseJson(nativeJson))
    }

    @Test
    fun testSpoutRejectsKryoRegisterLikeStorm() {
        val stormBuilder = TopologyBuilder()
        val stormDeclarer = stormBuilder.setSpout("spout", ConfigurableSpout(mutableMapOf()))
        assertFailsWith<IllegalArgumentException> {
            stormDeclarer.addConfigurations(mapOf(Config.TOPOLOGY_KRYO_REGISTER to "x"))
        }

        val nativeBuilder = KumulusTopologyBuilder()
        val nativeDeclarer = nativeBuilder.setSpout("spout", ConfigurableSpout(mutableMapOf()))
        assertFailsWith<IllegalArgumentException> {
            nativeDeclarer.addConfigurations(mapOf(Config.TOPOLOGY_KRYO_REGISTER to "x"))
        }
    }

    @Test
    fun testBoltRejectsKryoRegisterLikeStorm() {
        val stormBuilder = TopologyBuilder()
        val stormDeclarer = stormBuilder.setBolt("bolt", ConfigurableBolt(mutableMapOf()))
        assertFailsWith<IllegalArgumentException> {
            stormDeclarer.addConfigurations(mapOf(Config.TOPOLOGY_KRYO_REGISTER to "x"))
        }

        val nativeBuilder = KumulusTopologyBuilder()
        val nativeDeclarer = nativeBuilder.setBolt("bolt", ConfigurableBolt(mutableMapOf()))
        assertFailsWith<IllegalArgumentException> {
            nativeDeclarer.addConfigurations(mapOf(Config.TOPOLOGY_KRYO_REGISTER to "x"))
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseJson(json: String?): Map<String, Any?> {
        if (json == null) {
            return emptyMap()
        }
        return JSONValue.parseWithException(json) as Map<String, Any?>
    }

    private class ConfigurableSpout(
        private val componentConfig: MutableMap<String, Any?>
    ) : IRichSpout {
        override fun open(
            conf: MutableMap<Any?, Any?>?,
            context: TopologyContext?,
            collector: SpoutOutputCollector?
        ) = Unit

        override fun close() = Unit
        override fun activate() = Unit
        override fun deactivate() = Unit
        override fun nextTuple() = Unit
        override fun ack(msgId: Any?) = Unit
        override fun fail(msgId: Any?) = Unit
        override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
            declarer.declare(Fields("f"))
        }

        override fun getComponentConfiguration(): MutableMap<String, Any?> = componentConfig.toMutableMap()
    }

    private class ConfigurableBolt(
        private val componentConfig: MutableMap<String, Any?>
    ) : IRichBolt {
        override fun prepare(
            stormConf: MutableMap<Any?, Any?>?,
            context: TopologyContext?,
            collector: OutputCollector?
        ) = Unit

        override fun execute(input: Tuple) = Unit
        override fun cleanup() = Unit
        override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
            declarer.declare(Fields("f"))
        }

        override fun getComponentConfiguration(): MutableMap<String, Any?> = componentConfig.toMutableMap()
    }
}
