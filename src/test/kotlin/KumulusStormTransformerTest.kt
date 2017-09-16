import mu.KotlinLogging
import org.HdrHistogram.Histogram
import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.BasicOutputCollector
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
    }

    @Test
    fun test1() {
        val builder = org.apache.storm.topology.TopologyBuilder()

        val config = Utils.readStormConfig()

        val spout = object : BaseRichSpout() {
            var collector: SpoutOutputCollector? = null

            var i = 0

            override fun nextTuple() {
                if (i < TOTAL_ITERATIONS) {
                    i++
                    logger.debug { "nextTuple() called in ${this.hashCode()}" }
                    this.collector?.emit(listOf(i, System.nanoTime()), i)
                    if (i == TOTAL_ITERATIONS) {
                        finish.countDown()
                    }
                }
                Thread.sleep(1)
            }

            override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
                this.collector = collector
                start.compareAndSet(0L, System.currentTimeMillis())
            }

            override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
                declarer?.declare(Fields("index", "nano-time"))
            }
        }

        val bolt = object : BaseBasicBolt() {
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

                histogram.recordValue(tookNanos / 1000)

                count++

                if (index % (TOTAL_ITERATIONS/10) == 0) {
                    logger.info {
                        StringBuilder("[index: $index] Latency histogram values for " +
                                "${context.thisComponentId}/${context.thisTaskId}:\n").also { sb ->
                            LOG_PERCENTILES.forEach { percentile ->
                                val duration = histogram.getValueAtPercentile(percentile)
                                val countUnder = histogram.totalCount -
                                        histogram.getCountBetweenValues(0, duration)
                                sb.append("$percentile ($countUnder): ${toMillis(duration)}\n")
                            }
                        }
                    }
                }
            }

            fun toMillis(i: Long) : Double {
                return i / 1000.0
            }

            override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {}
        }

        builder.setSpout("spout", spout, 1)
        builder.setBolt("bolt", bolt).shuffleGrouping("spout")
        builder.setBolt("bolt2", bolt).shuffleGrouping("spout")

        val topology = builder.createTopology()!!

        val stormId = "testtopology"

        config.set(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1)
        config.set(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS, 0)
        config.set(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS, 1)
        config.set(Config.STORM_CLUSTER_MODE, "local")

        val kumulusTopology =
                KumulusStormTransformer.initializeTopology(builder, topology, config, stormId)
        kumulusTopology.prepare()
        kumulusTopology.start()
        finish.await()
        kumulusTopology.stop()

//        val cluster = LocalCluster()
//        cluster.submitTopology(stormId, config, topology)
//        finish.await()

        println("Done, took: ${System.currentTimeMillis() - start.get()}")
    }
}