package org.oryx.kumulus.collector

import mu.KotlinLogging
import org.apache.storm.generated.Grouping
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.tuple.Tuple
import org.apache.storm.utils.Utils
import org.oryx.kumulus.KumulusAcker
import org.oryx.kumulus.KumulusEmitter
import org.oryx.kumulus.KumulusTuple
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout
import org.oryx.kumulus.component.TupleImpl

abstract class KumulusCollector<T: KumulusComponent>(
        protected val component : KumulusComponent,
        private val componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>>,
        private val emitter: KumulusEmitter,
        protected val acker : KumulusAcker
) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private fun emit(
            streamId: String?,
            tuple: MutableList<Any>,
            messageId: Any?,
            anchors: Collection<Tuple>?
    ) : MutableList<Int> {
        val ret = mutableListOf<Int>()

        var executes: List<Pair<KumulusComponent, KumulusTuple>> = listOf()

        component.groupingStateMap[streamId]?.let { streamTargets: Map<String, CustomStreamGrouping> ->
            streamTargets.forEach { _, grouping ->
                val tasks = grouping.chooseTasks(this.component.taskId(), tuple)

                val emitToInstance= emitter.getDestinations(tasks)

                // First, expand all trees
                executes += emitToInstance.map { destComponent ->
                    val kumulusTuple = KumulusTuple(component, streamId ?: Utils.DEFAULT_STREAM_ID, tuple, anchors, messageId)
                    acker.expandTrees(component, destComponent.taskId(), kumulusTuple)
                    Pair(destComponent, kumulusTuple)
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
        return emit(streamId, tuple, messageId, anchors)
    }

    fun emit(streamId: String?, tuple: MutableList<Any>, messageId: Any?): MutableList<Int> {
        assert(component is KumulusSpout) { "Bolts wrong emit method called for '${component.context.thisComponentId}'" }
        acker.startTree(component as KumulusSpout, messageId)
        return emit(streamId, tuple, messageId!!, null)
    }
}
