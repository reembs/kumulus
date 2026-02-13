package org.xyro.kumulus

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import org.xyro.kumulus.topology.KumulusTopologyBuilder
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertTrue

class TestAllowExtraAckingMode {
    @Test
    fun testMultipleSpouts() {
        val builder = KumulusTopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = MAX_SPOUT_PENDING
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L
        config[KumulusTopology.CONF_EXTRA_ACKING] = true

        builder.setSpout("spout", TestSpout())
        builder.setSpout("spout2", TestSpout())
        builder.setSpout("spout3", TestSpout())

        builder
            .setBolt("acking-bolt", TestBolt())
            .allGrouping("spout")
            .allGrouping("spout2")
            .allGrouping("spout3")

        builder
            .setBolt("acking-bolt2", TestBolt())
            .allGrouping("spout")
            .allGrouping("spout2")
            .allGrouping("spout3")
        val kumulusTopology =
            KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test")
        kumulusTopology.prepare(10, TimeUnit.SECONDS)
        kumulusTopology.start()
        Thread.sleep(5000)

        // no asserts fail

        kumulusTopology.stop()

        logger.info { "Executed ${executions.get()} times, no errors" }
        assertTrue { executions.get() > 100 }
    }

    class TestSpout : DummySpout({ it.declare(Fields("id")) }) {
        override fun fail(msgId: Any?) {
            inFlight.decrementAndGet()
        }

        override fun ack(msgId: Any?) {
            inFlight.decrementAndGet()
        }

        override fun nextTuple() {
            val messageId = UUID.randomUUID().toString()
            collector.emit(listOf(messageId), messageId)
            val currentPending = inFlight.incrementAndGet()
            if (currentPending > MAX_SPOUT_PENDING) {
                logger.trace {
                    /*
                     * Kumulus does not guarantee max-spout-pending concurrency between ack/fail and nextTuple. That
                     * means that while it is not possible to breach max-spout-pending before all bolts in the
                     * tuple-tree had responded, the following scenario is possible:
                     *
                     * 1. All bolts responded
                     * 2. nextTuple() is called before ack/fail, seemingly breaching the guarantee from the spout's
                     *    perspective
                     * 3. ack/fail is called
                     *
                     * Note that this can cause currentPending to drift from max-spout-pending by much more than 1
                     */
                    "Current in-flight tuples ($currentPending) exceeded the max-spout-pending configuration ($MAX_SPOUT_PENDING)."
                }
            }
        }
    }

    class TestBolt : IRichBolt {
        private lateinit var collector: OutputCollector

        override fun execute(input: Tuple) {
            executions.incrementAndGet()
            collector.ack(input)
            collector.ack(input) // extra ack
        }

        override fun prepare(
            p0: MutableMap<Any?, Any?>?,
            p1: TopologyContext?,
            p2: OutputCollector,
        ) {
            this.collector = p2
        }

        override fun cleanup() = Unit

        override fun getComponentConfiguration(): MutableMap<String, Any> = mutableMapOf()

        override fun declareOutputFields(p0: OutputFieldsDeclarer) = Unit
    }

    companion object {
        private const val MAX_SPOUT_PENDING = 5L

        private val logger = KotlinLogging.logger {}
        private val executions = AtomicInteger(0)
        private val inFlight = AtomicInteger(0)
    }
}
