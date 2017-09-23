package org.oryx.kumulus.collector

import mu.KotlinLogging
import org.apache.storm.generated.Grouping
import org.apache.storm.task.IOutputCollector
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.KumulusAcker
import org.oryx.kumulus.KumulusEmitter
import org.oryx.kumulus.component.KumulusBolt
import org.oryx.kumulus.component.KumulusComponent

class KumulusBoltCollector(
        component: KumulusComponent,
        componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>>,
        emitter: KumulusEmitter,
        acker : KumulusAcker,
        errorHandler : ((Throwable?) -> Unit)?
) : KumulusCollector<KumulusBolt>(
        component,
        componentRegisteredOutputs,
        emitter,
        acker,
        errorHandler
), IOutputCollector {
    companion object {
        val logger = KotlinLogging.logger { }
    }

    override fun emitDirect(taskId: Int, streamId: String?, anchors: MutableCollection<Tuple>?, tuple: MutableList<Any>?) {
        TODO("not implemented")
    }

    override fun resetTimeout(input: Tuple?) {
        TODO("not implemented")
    }

    override fun fail(input: Tuple?) {
        acker.fail(component, input)
    }

    override fun ack(input: Tuple?) {
        acker.ack(component, input)
    }
}