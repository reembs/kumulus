package org.oryx.kumulus.collector

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
        acker : KumulusAcker
) : KumulusCollector<KumulusBolt>(component, componentRegisteredOutputs, emitter, acker), IOutputCollector {
    override fun emitDirect(taskId: Int, streamId: String?, anchors: MutableCollection<Tuple>?, tuple: MutableList<Any>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun resetTimeout(input: Tuple?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun fail(input: Tuple?) {
        acker.fail(component, input)
        component.inUse.set(false)
    }

    override fun reportError(error: Throwable?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun ack(input: Tuple?) {
        acker.ack(component, input)
        component.inUse.set(false)
    }
}