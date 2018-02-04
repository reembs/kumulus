package org.xyro.kumulus

import org.apache.storm.generated.Grouping

class KumulusTopologyContext {
    val targets: Map<String, MutableMap<String, Grouping>> = mapOf()
    val componentId: String = ""
    val taskId: Int = 0
    val outputFields: Map<String, List<String>> = mapOf()
    val componentTasks: List<Int> = listOf()
    val taskIndex: Int = 0

    constructor() {

    }
}