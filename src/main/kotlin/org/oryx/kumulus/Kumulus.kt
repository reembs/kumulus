/**
* Created by reem on 9/8/17.
*/

package org.oryx.kumulus

import clojure.lang.Atom
import org.apache.storm.Config
import org.apache.storm.LocalCluster
import org.apache.storm.generated.*
import org.apache.storm.metric.api.IMetric
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.*
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import java.util.*
import kotlin.collections.LinkedHashMap


fun main(args: Array<String>) {
    val builder = org.apache.storm.topology.TopologyBuilder()

    val config = Config()

    val spout = object : BaseRichSpout() {
        var collector: SpoutOutputCollector? = null

        override fun nextTuple() {
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

    builder.setSpout("spout", spout)
    builder.setBolt("bolt", bolt)
            .shuffleGrouping("spout")
    builder.setBolt("bolt2", bolt)
            .shuffleGrouping("spout")

    val topology = builder.createTopology()

    val stormId = "test-topology"

    config.set(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1)
    config.set(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS, 0)
    config.set(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS, 1)

    //val cluster = LocalCluster()
    //cluster.submitTopology(stormId, config, topology)

    init(builder, topology, config, stormId)

    Thread.sleep(1000 * 60 * 5)

    println("Done")
}

@Suppress("UNCHECKED_CAST")
fun init(builder: TopologyBuilder, topology: StormTopology?, config: Config, stormId: String) {
    val boltField = builder.javaClass.getDeclaredField("_bolts")
    boltField.isAccessible = true
    val boltsMap : Map<String, IRichBolt> = boltField.get(builder) as Map<String, IRichBolt>

    val spoutField = builder.javaClass.getDeclaredField("_spouts")
    spoutField.isAccessible = true
    val spoutsMap : Map<String, IRichSpout> = spoutField.get(builder) as Map<String, IRichSpout>

    val commonsField = builder.javaClass.getDeclaredField("_commons")
    commonsField.isAccessible = true
    val commons : Map<String, ComponentCommon> = commonsField.get(builder) as Map<String, ComponentCommon>

    data class ComponentInputRef(val component: IComponent, val grouping: Grouping)

    val taskToComponent = mutableMapOf<Int, String>()

    val componentToSortedTasks = mutableMapOf<String, List<Int>>()
    val componentToStreamToFields : Map<String, Map<String, Fields>> = HashMap()

    val inputRegistry = HashMap<GlobalStreamId, ComponentInputRef>()

    val codeDir = "/tmp"
    val pidDir = "/tmp"
    val workerPort = 6699
    val workerTasks : List<Int> = listOf()
    val defaultResources : Map<String, Any> = LinkedHashMap()
    val userResources : Map<String, Any> = LinkedHashMap()
    val executorData : Map<String, Any> = LinkedHashMap()
    val registeredMetrics : Map<Int, Map<Int, Map<String, IMetric>>> = LinkedHashMap()

    val context = TopologyContext(
            topology,
            config,
            taskToComponent,
            componentToSortedTasks,
            componentToStreamToFields,
            stormId,
            codeDir,
            pidDir,
            1,
            workerPort,
            workerTasks,
            defaultResources,
            userResources,
            executorData,
            registeredMetrics,
            Atom(Object()))

    var id = 1

    for ((name) in spoutsMap) {
        taskToComponent[id] = name

        val tasks : MutableList<Int> = if (!componentToSortedTasks.containsKey(name)) {
            mutableListOf()
        } else {
            componentToSortedTasks[name] as MutableList<Int>
        }

        tasks.add(id)

        val streamToFields : MutableMap<String, Fields> = if (componentToStreamToFields.containsKey(name)) {
            componentToStreamToFields[name] as MutableMap<String, Fields>
        } else {
            mutableMapOf()
        }

        val componentCommon = topology?._spouts?.get(name)?.getFieldValue(SpoutSpec._Fields.COMMON) as ComponentCommon
        for (stream in componentCommon._streams.keys) {
            val streamInfo = componentCommon._streams[stream]
            streamToFields[stream] = Fields(streamInfo?._output_fields)
        }

        id++
    }
}