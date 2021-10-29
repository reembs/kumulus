package org.xyro.kumulus

import mu.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Test
import java.util.concurrent.TimeUnit

class TestSingleAcking {
    @Test
    fun testSingleAckPerTreeTopology() {
        val builder = org.apache.storm.topology.TopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()

        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 5L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 5L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 5L

        builder.setSpout("spout", TestSpout())
        builder.setBolt("bolt1", TestBolt())
            .noneGrouping("spout")
        builder.setBolt("bolt2", TestBolt())
            .noneGrouping("spout")

        val stormTopology = builder.createTopology()!!
        val kumulusTopology =
            KumulusStormTransformer.initializeTopology(stormTopology, config, "test")
        kumulusTopology.prepare(10, TimeUnit.SECONDS)
        kumulusTopology.start(false)
        Thread.sleep(5000)
    }

    class TestSpout : DummySpout({ it.declare(Fields("id")) }) {
        private var index: Int = 0
        private val emitted = mutableSetOf<Int>()

        override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
            super.open(conf, context, collector)
            this.index = 0
        }

        override fun fail(msgId: Any?) {
            finish(msgId)
        }

        override fun ack(msgId: Any?) {
            finish(msgId)
        }

        private fun finish(msgId: Any?) {
            if ((msgId as Int) % 100_000 == 0) {
                logger.info { "Acking $msgId" }
            }
            if (!emitted.remove(msgId)) {
                throw RuntimeException("msgId '$msgId' had already been acked")
            }
        }

        override fun nextTuple() {
            val messageId = ++index
            emitted.add(messageId)
            collector.emit(listOf(messageId), messageId)
        }
    }

    class TestBolt : BaseBasicBolt() {
        override fun execute(p0: Tuple?, p1: BasicOutputCollector?) = Unit
        override fun declareOutputFields(p0: OutputFieldsDeclarer?) = Unit
    }

    companion object {
        private val logger = KotlinLogging.logger {}
    }
}
