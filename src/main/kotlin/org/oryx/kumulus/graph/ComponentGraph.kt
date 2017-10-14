package org.oryx.kumulus.graph

import com.fasterxml.jackson.databind.ObjectMapper
import org.oryx.kumulus.component.KumulusComponent

class ComponentGraph<out N : GraphNode, out E : GraphEdge<N>>(
        components: List<KumulusComponent>,
        nodeFactory: ComponentGraphNodeFactory<N>,
        edgeFactory: ComponentGraphEdgeFactory<N, E>
) {
    val edgeList: List<E>
    val nodeMap: Map<String, N>

    companion object {
        val mapper = ObjectMapper()
    }

    init {
        val componentsMap = mapOf(*components.map {
            it.componentId to it
        }.toTypedArray())

        val nodeMap: MutableMap<String, N> = mutableMapOf()

        fun getNode(componentName: String): N = nodeMap.computeIfAbsent(componentName, { name ->
            nodeFactory.createNode(componentsMap[name]!!)
        })

        this.edgeList = componentsMap.values.flatMap { cmp ->
            cmp.context.thisTargets.flatMap { (stream, groupings) ->
                groupings.map { (component, grouping) ->
                    edgeFactory.createEdge(stream!!, getNode(cmp.componentId), getNode(component!!), grouping!!)
                }
            }
        }

        this.nodeMap = nodeMap
    }

    fun toJson(): String {
        return mapper.writeValueAsString(
                mapper.createObjectNode()!!.apply {
                    this.withArray("nodes").let {
                        it.addAll(nodeMap.values.map { it.toJson() })
                    }
                    this.withArray("edges").let {
                        it.addAll(edgeList.map { it.toJson() })
                    }
                })
    }
}