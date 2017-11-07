package org.xyro.kumulus

import clojure.lang.Atom
import org.apache.storm.Config
import org.apache.storm.Constants
import org.apache.storm.generated.*
import org.apache.storm.metric.api.IMetric
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.*
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.utils.Utils
import org.xyro.kumulus.component.KumulusBolt
import org.xyro.kumulus.component.KumulusComponent
import org.xyro.kumulus.component.KumulusSpout
import java.io.Serializable

@Suppress("unused")
class KumulusStormTransformer {
    companion object {
        /**
         * Initialize a Kumulus topology from a {@link StormTopology}
         * @param topology the Storm topology to transform
         * @param rawConfig the Storm configuration
         * @param stormId the Storm topology ID string
         */
        @Suppress("UNCHECKED_CAST")
        @JvmStatic
        fun initializeTopology(topology: StormTopology?, rawConfig: MutableMap<String, Any>, stormId: String) : KumulusTopology {
            val boltField = StormTopology::class.java.getDeclaredField("bolts")!!
            boltField.isAccessible = true
            val serializedBoltsMap : Map<String, Bolt> = boltField.get(topology) as Map<String, Bolt>
            val boltsMap : Map<String, IComponent> = serializedBoltsMap.entries.associate { (id, bolt) ->
                val boltObject = bolt._bolt_object!!
                id to (Utils.javaDeserialize<Serializable>(boltObject._serialized_java, Serializable::class.java) as IComponent)
            }

            val spoutField = StormTopology::class.java.getDeclaredField("spouts")!!
            spoutField.isAccessible = true
            val serializedSpoutsMap : Map<String, SpoutSpec> = spoutField.get(topology) as Map<String, SpoutSpec>
            val spoutsMap : Map<String, IComponent> = serializedSpoutsMap.entries.associate { (id, spout) ->
                val spoutObject = spout._spout_object!!
                id to (Utils.javaDeserialize<Serializable>(spoutObject._serialized_java, Serializable::class.java) as IComponent)
            }

            val taskToComponent = mutableMapOf<Int, String>()

            val componentToSortedTasks = mutableMapOf<String, List<Int>>()
            val componentToStreamToFields = mutableMapOf<String, Map<String, Fields>>()

            rawConfig[Config.TOPOLOGY_NAME] = stormId

            componentToSortedTasks[Constants.SYSTEM_COMPONENT_ID] = listOf(Constants.SYSTEM_TASK_ID.toInt())
            componentToStreamToFields[Constants.SYSTEM_COMPONENT_ID] =
                    mapOf(Constants.SYSTEM_TICK_STREAM_ID to Fields())

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
                            kComponentInputs[name to it.key] = it.value
                        })

                        id++
                    }
                }
            }

            val config = rawConfig.mapValues { it ->
                return@mapValues (it.value as? Int)?.toLong() ?: it.value
            }

            componentToSortedTasks.forEach({ componentId: String, taskIds: List<Int> ->
                val componentObjectSerialized = if (topology?._spouts?.containsKey(componentId)!!) {
                    topology._spouts[componentId]?._spout_object
                } else {
                    topology._bolts[componentId]?._bolt_object
                }

                taskIds.forEach({ taskId ->
                    val componentInstance =
                            if (taskId == Constants.SYSTEM_TASK_ID.toInt()) {
                                BasicBoltExecutor(object : BaseBasicBolt() {
                                    override fun execute(input: Tuple?, collector: BasicOutputCollector?) {}
                                    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
                                        // Declared hard-coded
                                    }
                                })
                            } else {
                                Utils.javaDeserialize<Serializable>(componentObjectSerialized?._serialized_java, Serializable::class.java)
                            }

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
                        is IRichBolt ->
                            KumulusBolt(config, context, componentInstance).apply {
                                (componentInstance.componentConfiguration ?: mapOf())[Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS]?.let { secs ->
                                    assert(secs is Number)
                                    (secs as Number).let {
                                        this.tickSecs = secs
                                    }
                                }
                            }
                        is IRichSpout -> KumulusSpout(config, context, componentInstance)
                        else ->
                            throw Throwable("Component of type ${componentInstance::class.qualifiedName} is not acceptable by Kumulus")
                    }
                })
            })

            return KumulusTopology(kComponents, config)
        }
    }
}