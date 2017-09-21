package org.oryx.kumulus.grouping

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.task.WorkerTopologyContext
import java.util.*

class AllGrouping : CustomStreamGrouping {
    private lateinit var tasks: List<Int>

    override fun prepare(context: WorkerTopologyContext, stream: GlobalStreamId, targetTasks: List<Int>) {
        this.tasks = targetTasks.toList()
    }

    override fun chooseTasks(taskId: Int, values: List<Any>): List<Int> {
        return tasks
    }
}