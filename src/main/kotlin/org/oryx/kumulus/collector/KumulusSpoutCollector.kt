package org.oryx.kumulus.collector

import org.apache.storm.generated.Grouping
import org.apache.storm.spout.ISpoutOutputCollector
import org.oryx.kumulus.KumulusAcker
import org.oryx.kumulus.KumulusEmitter
import org.oryx.kumulus.component.KumulusComponent

class KumulusSpoutCollector(
        component: KumulusComponent,
        componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>>,
        emitter: KumulusEmitter,
        acker : KumulusAcker
) : KumulusCollector(component, componentRegisteredOutputs, emitter, acker), ISpoutOutputCollector {
    override fun emitDirect(taskId: Int, streamId: String?, tuple: MutableList<Any>?, messageId: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getPendingCount(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun reportError(error: Throwable?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}