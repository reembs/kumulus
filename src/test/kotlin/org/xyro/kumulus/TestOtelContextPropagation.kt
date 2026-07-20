package org.xyro.kumulus

import io.opentelemetry.api.GlobalOpenTelemetry
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
import kotlin.test.assertNotEquals

class TestOtelContextPropagation {
    companion object {
        // Accessed statically by inner classes — not serialized as bolt/spout instance fields.
        val signal = LinkedBlockingQueue<Unit>()

        private fun tracer() = GlobalOpenTelemetry.getTracer("kumulus-test")
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

    /**
     * Spout starts a span and makes it current before emitting. The bolt starts a child span
     * inside execute() with no explicit parent, so it inherits Context.current(). If the OTel
     * context crossed the queue/thread boundary, the child shares the spout span's traceId.
     * (A framework spout span may sit between the two in the hierarchy, so we assert same-trace
     * rather than a direct parent link.)
     */
    @Test
    fun testContextCrossesSpoutBoltBoundary() {
        signal.clear()
        withInMemorySdk { exporter ->
            val builder = KumulusTopologyBuilder()
            val config = mutableMapOf<String, Any>(Config.TOPOLOGY_MAX_SPOUT_PENDING to 1L)

            builder.setSpout("spout", TracingSpout())
            builder.setBolt("bolt", ChildSpanBolt()).noneGrouping("spout")

            val topology = KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
            topology.prepare(10, TimeUnit.SECONDS)
            topology.start(false)
            awaitSignal()
            topology.stop()

            val spans = exporter.finishedSpanItems
            val root = spans.first { it.name == "spout-root" }
            val child = spans.first { it.name == "bolt-child" }
            assertEquals(root.traceId, child.traceId, "bolt span must share the spout span's trace")
            assertNotEquals(root.spanId, child.spanId, "bolt span must be distinct from the spout span")
        }
    }

    // Starts "spout-root", makes it current, emits once inside the span scope.
    class TracingSpout : DummySpout({ it.declare(Fields("val")) }) {
        private var emitted = false

        override fun nextTuple() {
            if (!emitted) {
                emitted = true
                val span = tracer().spanBuilder("spout-root").startSpan()
                try {
                    span.makeCurrent().use {
                        collector.emit(listOf("v"), "msg-1")
                    }
                } finally {
                    span.end()
                }
            }
        }
    }

    // Starts "bolt-child" with implicit parent = Context.current(), signals, acks.
    class ChildSpanBolt : IRichBolt {
        private lateinit var collector: OutputCollector

        override fun prepare(
            conf: MutableMap<Any?, Any?>?,
            ctx: TopologyContext?,
            c: OutputCollector?,
        ) {
            collector = c!!
        }

        override fun execute(input: Tuple) {
            tracer().spanBuilder("bolt-child").startSpan().end()
            signal.offer(Unit)
            collector.ack(input)
        }

        override fun cleanup() = Unit

        override fun getComponentConfiguration() = mutableMapOf<String, Any>()

        override fun declareOutputFields(d: OutputFieldsDeclarer) = Unit
    }
}
