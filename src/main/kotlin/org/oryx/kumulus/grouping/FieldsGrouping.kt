package org.oryx.kumulus.grouping

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.task.WorkerTopologyContext

class FieldsGrouping(
        private val groupingFields: List<String>,
        private val outputFields: List<String>
) : CustomStreamGrouping {
    private lateinit var tasks: List<Int>

    override fun prepare(context: WorkerTopologyContext, stream: GlobalStreamId, targetTasks: List<Int>) {
        this.tasks = targetTasks.toList()
    }

    override fun chooseTasks(taskId: Int, values: List<Any>): List<Int> {
        var groupingHashes = 0L

        groupingFields.forEach { gField ->
            outputFields.let {
                val fieldValue = values[it.indexOf(gField)]
                groupingHashes += fieldValue.hashCode()
            }
        }

        return listOf(tasks[(groupingHashes % tasks.size).toInt()])
    }
}