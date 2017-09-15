package org.oryx.kumulus

import clojure.lang.Atom
import org.apache.storm.generated.ComponentCommon
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.StormTopology
import org.apache.storm.metric.api.IMetric
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IComponent
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.IRichSpout
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.apache.storm.utils.Utils
import org.oryx.kumulus.component.KumulusBolt
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout
import java.io.Serializable

@Suppress("unused")
class KumulusStormTransformer {
    companion object {
        @Suppress("UNCHECKED_CAST")
        fun initializeTopology(builder: TopologyBuilder, topology: StormTopology?, config: MutableMap<String, Any>, stormId: String) : KumulusTopology {
            val boltField = builder.javaClass.getDeclaredField("_bolts")
            boltField.isAccessible = true
            val boltsMap : Map<String, IComponent> = boltField.get(builder) as Map<String, IRichBolt>

            val spoutField = builder.javaClass.getDeclaredField("_spouts")
            spoutField.isAccessible = true
            val spoutsMap : Map<String, IComponent> = spoutField.get(builder) as Map<String, IRichSpout>

            val taskToComponent = mutableMapOf<Int, String>()

            val componentToSortedTasks = mutableMapOf<String, List<Int>>()
            val componentToStreamToFields = mutableMapOf<String, Map<String, Fields>>()

            val codeDir = "/tmp"
            val pidDir = "/tmp"
            val workerPort = 6699
            val workerTasks : List<Int> = listOf()
            val defaultResources : Map<String, Any> = LinkedHashMap()
            val userResources : Map<String, Any> = LinkedHashMap()
            val executorData : Map<String, Any> = LinkedHashMap()
            val registeredMetrics : Map<Int, Map<Int, Map<String, IMetric>>> = LinkedHashMap()

            val getter: (String) -> ComponentCommon = { id ->
                if (topology?._bolts?.containsKey(id)!!) {
                    topology._bolts[id]!!._common
                } else {
                    topology._spouts[id]!!._common
                }
            }

            val componentMaps = listOf(spoutsMap, boltsMap)

            val kComponents : MutableList<KumulusComponent> = mutableListOf()

            val kComponentInputs : MutableMap<Pair<String, GlobalStreamId>, org.apache.storm.generated.Grouping> =
                    mutableMapOf()

            var id = 1
            for (componentMap in componentMaps) {
                for ((name) in componentMap) {
                    val componentCommon = getter(name)
                    val parallelism = Math.max(componentCommon._parallelism_hint, 1)
                    for (i in 1..parallelism) {
                        taskToComponent[id] = name

                        val tasks: MutableList<Int> = if (!componentToSortedTasks.containsKey(name)) {
                            componentToSortedTasks[name] = mutableListOf()
                            componentToSortedTasks[name] as MutableList<Int>
                        } else {
                            componentToSortedTasks[name] as MutableList<Int>
                        }

                        tasks.add(id)

                        val streamToFields: MutableMap<String, Fields> = if (componentToStreamToFields.containsKey(name)) {
                            componentToStreamToFields[name] as MutableMap<String, Fields>
                        } else {
                            componentToStreamToFields[name] = mutableMapOf()
                            componentToStreamToFields[name] as MutableMap<String, Fields>
                        }

                        for (stream in componentCommon._streams.keys) {
                            val streamInfo = componentCommon._streams[stream]
                            streamToFields[stream] = Fields(streamInfo?._output_fields)
                        }

                        componentCommon._inputs?.forEach({
                            kComponentInputs[Pair(name, it.key)] = it.value
                        })

                        id++
                    }
                }
            }

            componentToSortedTasks.forEach({ componentId: String, taskIds: List<Int> ->
                val componentObjectSerialized = if (topology?._spouts?.containsKey(componentId)!!) {
                    topology._spouts[componentId]?._spout_object
                } else {
                    topology._bolts[componentId]?._bolt_object
                }

                taskIds.forEach({ taskId ->
                    val componentInstance = Utils.javaDeserialize<Serializable>(
                            componentObjectSerialized?._serialized_java, Serializable::class.java)

                    val context = TopologyContext(
                            topology,
                            config,
                            taskToComponent,
                            componentToSortedTasks,
                            componentToStreamToFields,
                            stormId,
                            codeDir,
                            pidDir,
                            taskId,
                            workerPort,
                            workerTasks,
                            defaultResources,
                            userResources,
                            executorData,
                            registeredMetrics,
                            Atom(Object()))

                    kComponents += when (componentInstance) {
                        is IRichBolt -> KumulusBolt(config, context, componentInstance)
                        is IRichSpout -> KumulusSpout(config, context, componentInstance)
                        else ->
                            throw Throwable("Component of type ${componentInstance::class.qualifiedName} is not acceptable by Kumulus")
                    }
                })
            })

            return KumulusTopology(kComponents, kComponentInputs, componentToStreamToFields, config)
        }
    }
}