import mu.KotlinLogging
import org.HdrHistogram.Histogram
import org.apache.storm.Config
import org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS
import org.apache.storm.Constants
import org.apache.storm.LocalCluster
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.FailedException
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.utils.Utils
import org.junit.Test
import org.oryx.kumulus.KumulusStormTransformer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

val LOG_PERCENTILES = arrayOf(5.0, 25.0, 50.0, 75.0, 90.0, 95.0, 98.0, 99.0, 99.9, 99.99)

internal class KumulusStormTransformerTest {
    companion object {
        @JvmStatic
        val finish = CountDownLatch(1)
        var start = AtomicLong(0)
        val TOTAL_ITERATIONS = 10000
        val SINK_BOLT_NAME = "bolt4"
    }

    @Test
    fun test1() {
        val builder = org.apache.storm.topology.TopologyBuilder()

        val config: MutableMap<String, Any> = mutableMapOf()

        val spout = object : BaseRichSpout() {
            var collector: SpoutOutputCollector? = null

            var i = 0

            override fun nextTuple() {
                if (i < TOTAL_ITERATIONS) {
                    i++
                    logger.debug { "nextTuple() called in ${this.hashCode()}" }
                    this.collector?.emit(listOf(i, System.nanoTime()), i)
                }
            }

            override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
                this.collector = collector
                start.compareAndSet(0L, System.currentTimeMillis())
            }

            override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
                declarer?.declare(Fields("index", "nano-time"))
            }

            override fun fail(msgId: Any?) {
                logger.error { "Got fail for $msgId" }

                if (msgId != null) {
                    logMsg(msgId)
                }

                super.fail(msgId)
            }

            override fun ack(msgId: Any?) {
                logger.trace { "Got ack for $msgId" }

                if (msgId != null) {
                    if (msgId as Int == TOTAL_ITERATIONS) {
                        finish.countDown()
                    }
                    logMsg(msgId)
                }

                super.ack(msgId)
            }

            private val seen = HashSet<Any>()

            fun logMsg(msgId: Any) {
                assert(seen.add(msgId)) { "MessageId $msgId was acked twice" }
            }
        }

        val bolt = TestBasicBolt()

        val failingBolt = TestBasicBolt(failing = true)

        val unanchoringBolt = object : IRichBolt {
            lateinit var collector: OutputCollector

            override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: OutputCollector?) {
                this.collector = collector!!
            }

            override fun execute(input: Tuple?) {
                try {
                    if (input!!.sourceStreamId == Constants.SYSTEM_TICK_STREAM_ID) {
                        logger.info { "Got tick tuple" }
                        return
                    }
                    collector.emit(input.values)
                } finally {
                    collector.ack(input)
                }
            }

            override fun cleanup() {}

            override fun getComponentConfiguration(): MutableMap<String, Any> {
                return mutableMapOf(Pair(TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1))
            }

            override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
                declarer?.declare(Fields("index", "nano-time"))
            }
        }

        val parallelism = 1
        val maxPending = 1
        builder.setSpout("spout", spout)
        builder.setBolt("bolt", bolt, parallelism)
                .shuffleGrouping("spout")
        builder.setBolt("bolt2", bolt, parallelism)
                .shuffleGrouping("bolt")
        builder.setBolt("bolt3", bolt, parallelism)
                .shuffleGrouping("bolt2")
        builder.setBolt(SINK_BOLT_NAME, bolt, parallelism)
                .shuffleGrouping("bolt3")

        builder.setBolt("unanchoring_bolt", unanchoringBolt, parallelism)
                .shuffleGrouping("bolt2")
        builder.setBolt("failing_bolt", failingBolt, parallelism)
                .shuffleGrouping("unanchoring_bolt")

        val topology = builder.createTopology()!!

        config.set(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1)
        config.set(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS, 0)
        config.set(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS, 1)
        config.set(Config.STORM_CLUSTER_MODE, "local")
        config.set(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxPending)

        val kumulusTopology =
                KumulusStormTransformer.initializeTopology(builder, topology, config, "testtopology")
        kumulusTopology.prepare()
        kumulusTopology.start()
        finish.await()
        kumulusTopology.stop()

//        val cluster = LocalCluster()
//        cluster.submitTopology("testtopology", config, topology)
//        finish.await()

        println("Done, took: ${System.currentTimeMillis() - start.get()}ms")
    }

    class TestBasicBolt(private val failing: Boolean = false) : BaseBasicBolt() {
        lateinit var context: TopologyContext
        lateinit var histogram: Histogram
        var count = 0

        override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?) {
            this.context = context!!
            histogram = Histogram(4)
            super.prepare(stormConf, context)
        }

        override fun execute(input: Tuple?, collector: BasicOutputCollector?) {
            val index: Int = input?.getValueByField("index") as Int
            val tookNanos = System.nanoTime() - input.getValueByField("nano-time") as Long

            logger.debug { "[${context.thisComponentId}/${context.thisTaskId}] " +
                    "Index: $index, took: ${tookNanos / 1000.0 / 1000.0}ms" }

            count++

            if (context.thisComponentId == SINK_BOLT_NAME) {
                histogram.recordValue(tookNanos / 1000)

                if (index == TOTAL_ITERATIONS) {
                    logger.info {
                        StringBuilder("[index: $index] Latency histogram values for " +
                                "${context.thisComponentId}/${context.thisTaskId}:\n").also { sb ->
                            LOG_PERCENTILES.forEach { percentile ->
                                val duration = histogram.getValueAtPercentile(percentile)
                                val countUnder = histogram.getCountBetweenValues(0, duration)
                                sb.append("$percentile ($countUnder): ${toMillis(duration)}ms\n")
                            }
                        }
                    }
                }
            }

            collector?.emit(input.values)

            if (failing) {
                throw FailedException()
            }
        }

        private fun toMillis(i: Long) : Double {
            return i / 1000.0
        }

        override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
            declarer?.declare(Fields("index", "nano-time"))
        }
    }
}