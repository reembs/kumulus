package org.oryx.kumulus.collector

import org.apache.storm.generated.Grouping
import org.apache.storm.spout.ISpoutOutputCollector
import org.oryx.kumulus.KumulusEmitter

class KumulusSpoutCollector(
        componentRegisteredOutputs: Map<String, Pair<String, Grouping>>,
        emitter: KumulusEmitter
) : KumulusCollector(componentRegisteredOutputs, emitter), ISpoutOutputCollector {

    override fun emitDirect(taskId: Int, streamId: String?, tuple: MutableList<Any>?, messageId: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getPendingCount(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun reportError(error: Throwable?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun emit(streamId: String?, tuple: MutableList<Any>?, messageId: Any?): MutableList<Int> {
        return super.emit(streamId, tuple)
    }
}