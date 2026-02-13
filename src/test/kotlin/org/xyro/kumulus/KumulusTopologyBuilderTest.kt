package org.xyro.kumulus

import org.apache.storm.tuple.Fields
import org.junit.Test
import org.xyro.kumulus.KumulusStormTransformer.KumulusTopologyValidationException
import org.xyro.kumulus.topology.KumulusTopologyBuilder

class KumulusTopologyBuilderTest {
    @Test
    fun testNativeTopologyBuilderHappyPath() {
        val builder = KumulusTopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        builder.setSpout("spout", DummySpout())
        builder.setBolt(
            "bolt",
            DummyBolt {
                it.declareStream("stream", Fields("num"))
            }
        ).noneGrouping("spout")
        builder.setBolt("bolt2", DummyBolt())
            .fieldsGrouping("bolt", "stream", Fields("num"))

        KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
    }

    @Test(expected = KumulusTopologyValidationException::class)
    fun testNativeTopologyBuilderValidation() {
        val builder = KumulusTopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        builder.setSpout("spout", DummySpout())
        builder.setBolt("bolt", DummyBolt())
            .noneGrouping("missing-bolt")

        KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
    }

    @Test(expected = IllegalArgumentException::class)
    fun testNativeTopologyBuilderRejectsDuplicateComponentId() {
        val builder = KumulusTopologyBuilder()

        builder.setSpout("component", DummySpout())
        builder.setBolt("component", DummyBolt())
    }

    @Test(expected = IllegalArgumentException::class)
    fun testNativeTopologyBuilderRejectsDuplicateComponentIdReverseOrder() {
        val builder = KumulusTopologyBuilder()

        builder.setBolt("component", DummyBolt())
        builder.setSpout("component", DummySpout())
    }

    @Test(expected = IllegalArgumentException::class)
    fun testNativeTopologyBuilderRejectsNonPositiveSpoutParallelism() {
        val builder = KumulusTopologyBuilder()

        builder.setSpout("spout", DummySpout(), 0)
    }

    @Test(expected = IllegalArgumentException::class)
    fun testNativeTopologyBuilderRejectsNonPositiveBoltParallelism() {
        val builder = KumulusTopologyBuilder()

        builder.setBolt("bolt", DummyBolt(), 0)
    }

    @Test(expected = IllegalArgumentException::class)
    fun testNativeTopologyBuilderRejectsNullWorkerHook() {
        val builder = KumulusTopologyBuilder()

        builder.addWorkerHook(null)
    }
}
