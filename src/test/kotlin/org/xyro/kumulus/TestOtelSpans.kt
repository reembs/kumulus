package org.xyro.kumulus

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.topology.KumulusTopologyBuilder
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TestOtelSpans {
    companion object {
        // Accessed statically by inner classes — not serialized as bolt instance fields.
        val signal = LinkedBlockingQueue<Unit>()
    }

    private fun awaitSignal() = signal.poll(5, TimeUnit.SECONDS) ?: error("timeout waiting for bolt execution")

    private fun withInMemorySdk(block: (InMemorySpanExporter) -> Unit) {
        val exporter = InMemorySpanExporter.create()
        val provider =
            SdkTracerProvider
                .builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build()
        val sdk = OpenTelemetrySdk.builder().setTracerProvider(provider).build()
        GlobalOpenTelemetry.resetForTest()
        GlobalOpenTelemetry.set(sdk)
        try {
            block(exporter)
        } finally {
            provider.close()
            GlobalOpenTelemetry.resetForTest()
        }
    }

    private fun runTopology(
        config: MutableMap<String, Any>,
        spout: DummySpout,
        bolt: IRichBolt,
    ) {
        signal.clear()
        val builder = KumulusTopologyBuilder()
        builder.setSpout("spout", spout)
        builder.setBolt("bolt", bolt).noneGrouping("spout")
        val topology = KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
        topology.prepare(10, TimeUnit.SECONDS)
        topology.start(false)
        awaitSignal()
        topology.stop()
    }

    private fun anchoredSpout() =
        object : DummySpout({ it.declare(Fields("val")) }) {
            private var emitted = false

            override fun nextTuple() {
                if (!emitted) {
                    emitted = true
                    collector.emit(listOf("v"), "msg-1")
                }
            }
        }

    @Test
    fun testRootSpanCreatedWithoutBoltSpans() {
        withInMemorySdk { exporter ->
            runTopology(mutableMapOf(Config.TOPOLOGY_MAX_SPOUT_PENDING to 1L), anchoredSpout(), AckBolt())

            val root = exporter.finishedSpanItems.firstOrNull { it.name == "kumulus.spout spout" }
            assertNotNull(root, "spout root span must be exported")
            assertNull(
                exporter.finishedSpanItems.firstOrNull { it.name.startsWith("kumulus.bolt") },
                "no bolt span when the flag is off",
            )
        }
    }

    @Test
    fun testBoltSpanIsChildOfRootWhenEnabled() {
        withInMemorySdk { exporter ->
            runTopology(
                mutableMapOf(
                    Config.TOPOLOGY_MAX_SPOUT_PENDING to 1L,
                    KumulusTopology.CONF_TRACING_BOLT_SPANS_ENABLED to true,
                ),
                anchoredSpout(),
                AckBolt(),
            )

            val root = exporter.finishedSpanItems.first { it.name == "kumulus.spout spout" }
            val boltSpan = exporter.finishedSpanItems.first { it.name == "kumulus.bolt bolt" }
            assertEquals(root.traceId, boltSpan.traceId, "bolt span shares the root trace")
            assertEquals(root.spanId, boltSpan.parentSpanId, "bolt span is a child of the root span")
        }
    }

    @Test
    fun testRootSpanErrorOnFail() {
        withInMemorySdk { exporter ->
            runTopology(mutableMapOf(Config.TOPOLOGY_MAX_SPOUT_PENDING to 1L), anchoredSpout(), FailBolt())

            val root = exporter.finishedSpanItems.first { it.name == "kumulus.spout spout" }
            assertEquals(StatusCode.ERROR, root.status.statusCode, "failed tuple tree marks the root span ERROR")
        }
    }

    @Test
    fun testUnanchoredRootSpanEnded() {
        withInMemorySdk { exporter ->
            val spout =
                object : DummySpout({ it.declare(Fields("val")) }) {
                    private var emitted = false

                    override fun nextTuple() {
                        if (!emitted) {
                            emitted = true
                            collector.emit(listOf("v")) // no messageId -> unanchored
                        }
                    }
                }
            runTopology(mutableMapOf(Config.TOPOLOGY_MAX_SPOUT_PENDING to 1L), spout, AckBolt())

            // Ended spans are the only ones exported; presence proves no leak on the unanchored path.
            assertTrue(
                exporter.finishedSpanItems.any { it.name == "kumulus.spout spout" },
                "unanchored spout span must still be ended",
            )
        }
    }

    // Acks then signals, so the signal implies the ack (and thus root-span end) has been processed.
    class AckBolt : IRichBolt {
        private lateinit var collector: OutputCollector

        override fun prepare(
            conf: MutableMap<Any?, Any?>?,
            ctx: TopologyContext?,
            c: OutputCollector?,
        ) {
            collector = c!!
        }

        override fun execute(input: Tuple) {
            collector.ack(input)
            signal.offer(Unit)
        }

        override fun cleanup() = Unit

        override fun getComponentConfiguration() = mutableMapOf<String, Any>()

        override fun declareOutputFields(d: OutputFieldsDeclarer) = Unit
    }

    // Fails then signals.
    class FailBolt : IRichBolt {
        private lateinit var collector: OutputCollector

        override fun prepare(
            conf: MutableMap<Any?, Any?>?,
            ctx: TopologyContext?,
            c: OutputCollector?,
        ) {
            collector = c!!
        }

        override fun execute(input: Tuple) {
            collector.fail(input)
            signal.offer(Unit)
        }

        override fun cleanup() = Unit

        override fun getComponentConfiguration() = mutableMapOf<String, Any>()

        override fun declareOutputFields(d: OutputFieldsDeclarer) = Unit
    }
}
