package org.oryx.kumulus.graph

import org.apache.storm.generated.Grouping
import org.oryx.kumulus.component.KumulusComponent

@JvmField
val defaultNodeFactory = object : ComponentGraphNodeFactory<GraphNode> {
    override fun createNode(component: KumulusComponent): GraphNode {
        return GraphNode(component)
    }
}

@JvmField
val defaultEdgeFactory = object : ComponentGraphEdgeFactory<GraphNode, GraphEdge<GraphNode>> {
    override fun createEdge(stream: String, src: GraphNode, dest: GraphNode, grouping: Grouping): GraphEdge<GraphNode> {
        return GraphEdge(stream, src, dest, grouping)
    }
}

interface ComponentGraphNodeFactory<out N: GraphNode> {
    fun createNode(component : KumulusComponent) : N
}


interface ComponentGraphEdgeFactory<N: GraphNode, out E: GraphEdge<N>> {
    fun createEdge(stream : String, src : N, dest : N, grouping : Grouping) : E
}

