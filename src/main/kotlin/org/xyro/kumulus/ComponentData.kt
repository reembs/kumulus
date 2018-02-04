package org.xyro.kumulus

import org.apache.storm.topology.IComponent
import org.apache.storm.tuple.Fields

interface ComponentData {
    fun createInstance() : IComponent
    fun getParallelism() : Int
    fun createContext(
            config: Map<String, Any>,
            taskToComponent: Map<Int, String>,
            componentToSortedTasks: Map<String, List<Int>>,
            componentToStreamToFields: Map<String, Map<String, Fields>>,
            stormId: String,
            taskId: Int
    ): Any
}