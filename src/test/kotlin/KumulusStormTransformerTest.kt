import org.apache.storm.Config
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


internal class KumulusStormTransformerTest {
    @Test
    fun test1() {
        val builder = org.apache.storm.topology.TopologyBuilder()

        val config = Utils.readStormConfig()

        val spout = object : BaseRichSpout() {
            var collector: SpoutOutputCollector? = null

            override fun nextTuple() {
                println("nextTuple() called in ${this.hashCode()}")
                val msgId = (Math.random() * 100000).toInt()
                this.collector?.emit(listOf("yo $msgId", System.nanoTime()), msgId)
                Thread.sleep(1000)
            }

            override fun open(conf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: SpoutOutputCollector?) {
                this.collector = collector
            }

            override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
                declarer?.declare(Fields("message", "nanotime"));
            }
        }

        val bolt = object : BaseBasicBolt() {
            override fun execute(input: Tuple?, collector: BasicOutputCollector?) {
                val message: String = input?.getValueByField("message") as String
                val nanotime: Long = input.getValueByField("nanotime") as Long
                println("Message: $message, took: ${(System.nanoTime() - nanotime) / 1000.0 / 1000.0}ms")
            }

            override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {}
        }

        builder.setSpout("spout", spout, 2)
        builder.setBolt("bolt", bolt, 5)
                .shuffleGrouping("spout")
        builder.setBolt("bolt2", bolt)
                .shuffleGrouping("spout")

        val topology = builder.createTopology()

        val stormId = "test-topology"

        config.set(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1)
        config.set(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS, 0)
        config.set(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS, 1)

        val kumulusTopology = KumulusStormTransformer.initializeTopology(builder, topology, config, stormId)
        kumulusTopology.prepare()
        kumulusTopology.start()


//    val cluster = LocalCluster()
//    cluster.submitTopology(stormId, config, topology)
        Thread.sleep(1000 * 60 * 5)

        kumulusTopology.stop()

        println("Done")
    }
}