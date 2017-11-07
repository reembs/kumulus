package org.xyro.kumulus.graph

import com.fasterxml.jackson.databind.node.ObjectNode
import org.xyro.kumulus.component.KumulusComponent

open class GraphNode(val component : KumulusComponent) {
    open fun toJson() : ObjectNode {
        return toJson(ComponentGraph.mapper.createObjectNode())
    }

    open protected fun toJson(nodeJson: ObjectNode) : ObjectNode {
        return nodeJson.apply {
            this.put("componentId", this@GraphNode.component.componentId)
        }
    }
}