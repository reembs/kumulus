package org.xyro.kumulus

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.KumulusStormTransformer
import org.xyro.kumulus.KumulusStormTransformer.*

class TestTopologyValidation {
    @Test(expected = KumulusTopologyValidationException::class)
    fun testMissingTargetBolt() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()
        builder.setSpout("spout", DummySpout())
        builder.setBolt("bolt", DummyBolt())
                .noneGrouping("missing-bolt")
        val stormTopology = builder.createTopology()!!
        KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
    }

    @Test(expected = KumulusTopologyValidationException::class)
    fun testMissingTargetStream() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()
        builder.setSpout("spout", DummySpout())

        builder.setBolt("bolt", DummyBolt())

        builder.setBolt("bolt2", DummyBolt())
                .noneGrouping("bolt", "missing-stream")

        val stormTopology = builder.createTopology()!!
        KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
    }

    @Test(expected = KumulusTopologyValidationException::class)
    fun testMissingTargetField() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()
        builder.setSpout("spout", DummySpout())

        builder.setBolt("bolt", DummyBolt({
            it.declareStream("stream", Fields("num"))
        })).noneGrouping("spout")

        builder.setBolt("bolt2", DummyBolt())
                .fieldsGrouping("bolt", "stream", Fields("num", "non-existing-field"))

        val stormTopology = builder.createTopology()!!
        KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
    }

    @Test
    fun testOkay() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()
        builder.setSpout("spout", DummySpout())

        builder.setBolt("bolt", DummyBolt({
            it.declareStream("stream", Fields("num"))
        })).noneGrouping("spout")

        builder.setBolt("bolt2", DummyBolt())
                .fieldsGrouping("bolt", "stream", Fields("num"))

        val stormTopology = builder.createTopology()!!
        KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
    }
}

open class DummySpout : BaseRichSpout {
    private val declare: (declarer: OutputFieldsDeclarer) -> Unit
    protected lateinit var collector: SpoutOutputCollector

    constructor() : this({})
    constructor(declare: (declarer: OutputFieldsDeclarer) -> Unit) : super() {
        this.declare = declare
    }

    override fun nextTuple() {}

    override fun open(conf: MutableMap<String, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
        this.collector = collector!!
    }
    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declare(declarer)
    }
}

open class DummyBolt : BaseBasicBolt {
    private val declare: (declarer: OutputFieldsDeclarer) -> Unit

    constructor() : this({})
    constructor(declare: (declarer: OutputFieldsDeclarer) -> Unit) : super() {
        this.declare = declare
    }

    override fun execute(input: Tuple, collector: BasicOutputCollector) {}
    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declare(declarer)
    }
}