package org.xyro.kumulus.collector

import mu.KotlinLogging
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.tuple.Tuple
import org.apache.storm.utils.Utils
import org.xyro.kumulus.KumulusAcker
import org.xyro.kumulus.KumulusEmitter
import org.xyro.kumulus.KumulusTuple
import org.xyro.kumulus.component.KumulusComponent
import org.xyro.kumulus.component.KumulusSpout
import org.xyro.kumulus.component.TupleImpl

abstract class KumulusCollector<T: KumulusComponent>(
        protected val component: KumulusComponent,
        private val emitter: KumulusEmitter,
        protected val acker: KumulusAcker,
        private val errorHandler: ((String, Int, Throwable) -> Unit)? = null
) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    // Impl. org.apache.storm.task.IOutputCollector
    fun reportError(error: Throwable?) {
        val reportError = error ?:
                Exception("reportError was called with null error. An error in component might be shadowed")
        errorHandler?.let {
            it(this.component.componentId, this.component.taskId, reportError)
        } ?: logger.error("An error was reported from bolt " +
                "${component.componentId}/${component.taskId}", reportError)
    }

    private fun componentEmit(
            streamId: String?,
            tuple: MutableList<Any>,
            messageId: Any?
    ) : MutableList<Int> {
        val ret = mutableListOf<Int>()

        var executes: List<Pair<KumulusComponent, KumulusTuple>> = listOf()

        component.groupingStateMap[streamId]?.let { streamTargets: Map<String, CustomStreamGrouping> ->
            streamTargets.forEach { _, grouping ->
                val tasks = grouping.chooseTasks(this.component.taskId, tuple)

                val emitToInstance= emitter.getDestinations(tasks)

                // First, expand all trees
                executes += emitToInstance.map { destComponent ->
                    val kumulusTuple = KumulusTuple(component, streamId ?: Utils.DEFAULT_STREAM_ID, tuple, messageId)
                    acker.expandTrees(component, destComponent.taskId, kumulusTuple)
                    destComponent to kumulusTuple
                }.toList()

                logger.trace { "Finished emitting from bolt $component" }

                ret += tasks
            }
        }

        /* Only after expending can we execute next bolts to prevent race that
                   would cause premature acking with the spout */
        executes.forEach { (component, tuple) ->
            emitter.execute(component, tuple)
        }

        return ret
    }

    fun emit(streamId: String?, anchors: MutableCollection<Tuple>?, tuple: MutableList<Any>): MutableList<Int> {
        val messageId = anchors
                ?.map { (it as TupleImpl).spoutMessageId }
                ?.toSet()
                ?.filter { it != null }
                ?.apply {
                    assert(this.size <= 1) { "Found more than a single message ID in emitted anchors: $anchors" }
                }
                ?.firstOrNull()
        return componentEmit(streamId, tuple, messageId)
    }

    fun emit(streamId: String?, tuple: MutableList<Any>, messageId: Any?): MutableList<Int> {
        assert(component is KumulusSpout) { "Bolts wrong emit method called for ${component.componentId}/${component.taskId}" }
        acker.startTree(component as KumulusSpout, messageId)
        return componentEmit(streamId, tuple, messageId!!)
    }
}
