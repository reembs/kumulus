package org.xyro.kumulus

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.test.appender.ListAppender
import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.slf4j.MDC
import org.xyro.kumulus.topology.KumulusTopologyBuilder
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

class TestLoggingContext {
    companion object {
        // Accessed statically by inner classes — not serialized as bolt/spout instance fields.
        val signal = LinkedBlockingQueue<Unit>()
        val spoutSignal = LinkedBlockingQueue<Unit>()
    }

    private fun awaitSignal() = signal.poll(5, TimeUnit.SECONDS) ?: error("timeout waiting for bolt execution")

    private fun awaitSpoutSignal() = spoutSignal.poll(5, TimeUnit.SECONDS) ?: error("timeout waiting for spout signal")

    private fun withListAppender(block: (ListAppender) -> Unit) {
        val appender = ListAppender("test-${System.nanoTime()}")
        appender.start()
        val ctx = LogManager.getContext(false) as LoggerContext
        val config = ctx.configuration
        config.addAppender(appender)
        config.rootLogger.addAppender(appender, Level.TRACE, null)
        ctx.updateLoggers()
        try {
            block(appender)
        } finally {
            config.rootLogger.removeAppender(appender.name)
            appender.stop()
            ctx.updateLoggers()
        }
    }

    private fun boltEvent(appender: ListAppender) = appender.events.last { "bolt-execute" in it.message.formattedMessage }

    @Test
    fun testFrameworkKeysPopulatedOnBolt() {
        signal.clear()
        withListAppender { appender ->
            val builder = KumulusTopologyBuilder()
            val config = mutableMapOf<String, Any>(Config.TOPOLOGY_MAX_SPOUT_PENDING to 1L)

            builder.setSpout(
                "my-spout",
                object : DummySpout({ it.declare(Fields("val")) }) {
                    private var emitted = false

                    override fun nextTuple() {
                        if (!emitted) {
                            emitted = true
                            collector.emit(listOf("data"), "msg-42")
                        }
                    }
                },
            )
            builder.setBolt("my-bolt", SignalingBolt()).noneGrouping("my-spout")

            val topology = KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
            topology.prepare(10, TimeUnit.SECONDS)
            topology.start(false)
            awaitSignal()
            topology.stop()

            val event = boltEvent(appender)
            assertEquals("my-bolt", event.contextMap["component"])
            assertEquals("default", event.contextMap["stream_id"])
            assertEquals("msg-42", event.contextMap["message_id"])
        }
    }

    @Test
    fun testCustomKeyPropagatesFromSpoutToBolt() {
        signal.clear()
        withListAppender { appender ->
            val builder = KumulusTopologyBuilder()
            val config = mutableMapOf<String, Any>(Config.TOPOLOGY_MAX_SPOUT_PENDING to 1L)

            builder.setSpout(
                "spout",
                object : DummySpout({ it.declare(Fields("val")) }) {
                    private var emitted = false

                    override fun nextTuple() {
                        if (!emitted) {
                            emitted = true
                            MDC.put("request_id", "req-999")
                            collector.emit(listOf("data"), "msg-1")
                        }
                    }
                },
            )
            builder.setBolt("bolt", SignalingBolt()).noneGrouping("spout")

            val topology = KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
            topology.prepare(10, TimeUnit.SECONDS)
            topology.start(false)
            awaitSignal()
            topology.stop()

            assertEquals("req-999", boltEvent(appender).contextMap["request_id"])
        }
    }

    @Test
    fun testMdcClearedBetweenBoltExecutions() {
        signal.clear()
        withListAppender { appender ->
            val builder = KumulusTopologyBuilder()
            val config = mutableMapOf<String, Any>(Config.TOPOLOGY_MAX_SPOUT_PENDING to 2L)

            builder.setSpout(
                "spout",
                object : DummySpout({ it.declare(Fields("val")) }) {
                    private var count = 0

                    override fun nextTuple() {
                        count++
                        when (count) {
                            1 -> {
                                MDC.put("unique_key", "only-in-msg1")
                                collector.emit(listOf("v"), "msg-1")
                            }
                            2 -> collector.emit(listOf("v"), "msg-2")
                        }
                    }
                },
            )
            builder.setBolt("bolt", SignalingBolt()).noneGrouping("spout")

            val topology = KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
            topology.prepare(10, TimeUnit.SECONDS)
            topology.start(false)
            awaitSignal()
            awaitSignal()
            topology.stop()

            val eventForMsg2 =
                appender.events
                    .filter { "bolt-execute" in it.message.formattedMessage }
                    .first { it.contextMap["message_id"] == "msg-2" }
            assertNull(eventForMsg2.contextMap["unique_key"], "unique_key from msg-1 must not leak into msg-2 execution")
        }
    }

    @Test
    fun testMdcClearedBetweenSpoutNextTupleCalls() {
        spoutSignal.clear()
        withListAppender { appender ->
            val builder = KumulusTopologyBuilder()
            val config = mutableMapOf<String, Any>(Config.TOPOLOGY_MAX_SPOUT_PENDING to 5L)

            builder.setSpout("spout", MdcLeakTestSpout())
            builder.setBolt("bolt", DummyBolt()).noneGrouping("spout")

            val topology = KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
            topology.prepare(10, TimeUnit.SECONDS)
            topology.start(false)
            awaitSpoutSignal()
            topology.stop()

            val event = appender.events.last { "spout-second-call" in it.message.formattedMessage }
            assertFalse(
                event.contextMap.containsKey("spout_key"),
                "spout_key set during first nextTuple must not leak into second nextTuple call",
            )
        }
    }

    @Test
    fun testContextFlowsThroughBoltChain() {
        signal.clear()
        withListAppender { appender ->
            val builder = KumulusTopologyBuilder()
            val config = mutableMapOf<String, Any>(Config.TOPOLOGY_MAX_SPOUT_PENDING to 1L)

            builder.setSpout(
                "spout",
                object : DummySpout({ it.declare(Fields("val")) }) {
                    private var emitted = false

                    override fun nextTuple() {
                        if (!emitted) {
                            emitted = true
                            MDC.put("request_id", "chain-req")
                            collector.emit(listOf("v"), "chain-msg")
                        }
                    }
                },
            )
            builder.setBolt("bolt1", HopAnnotatingBolt()).noneGrouping("spout")
            builder.setBolt("bolt2", SignalingBolt()).noneGrouping("bolt1")

            val topology = KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
            topology.prepare(10, TimeUnit.SECONDS)
            topology.start(false)
            awaitSignal()
            topology.stop()

            val event = boltEvent(appender)
            assertEquals("chain-req", event.contextMap["request_id"])
            assertEquals("1", event.contextMap["hop_count"])
            assertEquals("bolt2", event.contextMap["component"])
            assertEquals("chain-msg", event.contextMap["message_id"])
        }
    }

    // Logs "bolt-execute" and signals via companion queue — context on the log event is what we assert.
    class SignalingBolt : IRichBolt {
        private lateinit var collector: OutputCollector

        override fun prepare(
            conf: MutableMap<Any?, Any?>?,
            ctx: TopologyContext?,
            c: OutputCollector?,
        ) {
            collector = c!!
        }

        override fun execute(input: Tuple) {
            logger.info("bolt-execute")
            signal.offer(Unit)
            collector.ack(input)
        }

        override fun cleanup() = Unit

        override fun getComponentConfiguration() = mutableMapOf<String, Any>()

        override fun declareOutputFields(d: OutputFieldsDeclarer) = Unit

        companion object {
            private val logger = KotlinLogging.logger {}
        }
    }

    // Adds hop_count=1 to MDC and emits downstream with anchoring.
    class HopAnnotatingBolt : IRichBolt {
        private lateinit var collector: OutputCollector

        override fun prepare(
            conf: MutableMap<Any?, Any?>?,
            ctx: TopologyContext?,
            c: OutputCollector?,
        ) {
            collector = c!!
        }

        override fun execute(input: Tuple) {
            MDC.put("hop_count", "1")
            collector.emit(listOf(input), input.values)
            collector.ack(input)
        }

        override fun cleanup() = Unit

        override fun getComponentConfiguration() = mutableMapOf<String, Any>()

        override fun declareOutputFields(d: OutputFieldsDeclarer) {
            d.declare(Fields("val"))
        }
    }

    // First nextTuple sets spout_key and emits; second logs "spout-second-call" and signals.
    class MdcLeakTestSpout : DummySpout({ it.declare(Fields("val")) }) {
        private var count = 0

        override fun nextTuple() {
            count++
            when (count) {
                1 -> {
                    MDC.put("spout_key", "from-first-call")
                    collector.emit(listOf("v"), "msg-1")
                }
                2 -> {
                    logger.info("spout-second-call")
                    spoutSignal.offer(Unit)
                }
            }
        }

        companion object {
            private val logger = KotlinLogging.logger {}
        }
    }
}
