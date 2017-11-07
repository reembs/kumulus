package org.xyro.kumulus.graph

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.storm.generated.Grouping

open class GraphEdge<out T: GraphNode>(
        val stream : String,
        val src : T,
        val dest : T,
        val grouping : Grouping
) {
    open fun toJson() : ObjectNode {
        return toJson(ComponentGraph.mapper.createObjectNode())
    }

    open protected fun toJson(edgeJson : ObjectNode) : ObjectNode {
        edgeJson.put("stream", this.stream)
        edgeJson.put("grouping", getGroupingName(this.grouping))
        edgeJson.put("src", this.src.component.componentId)
        edgeJson.put("dest", this.dest.component.componentId)
        return edgeJson
    }

    protected open fun getGroupingName(grouping: Grouping): String {
        return if (grouping.is_set_all) {
            "all"
        } else if (grouping.is_set_none || grouping.is_set_shuffle || grouping.is_set_local_or_shuffle) {
            "shuffle"
        } else if (grouping.is_set_fields) {
            "fields"
        } else if  (grouping.is_set_custom_serialized) {
            "custom"
        } else {
            "other"
        }
    }
}