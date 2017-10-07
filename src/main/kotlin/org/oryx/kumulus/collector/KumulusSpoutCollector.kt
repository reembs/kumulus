package org.oryx.kumulus.collector

import org.apache.storm.spout.ISpoutOutputCollector
import org.oryx.kumulus.KumulusAcker
import org.oryx.kumulus.KumulusEmitter
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout

class KumulusSpoutCollector(
        component: KumulusComponent,
        emitter: KumulusEmitter,
        acker: KumulusAcker,
        errorHandler: ((String, Int, Throwable) -> Unit)?
) : KumulusCollector<KumulusSpout>(
        component,
        emitter,
        acker,
        errorHandler
), ISpoutOutputCollector {
    override fun emitDirect(taskId: Int, streamId: String?, tuple: MutableList<Any>?, messageId: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getPendingCount(): Long {
        return acker.getPendingCount()
    }
}