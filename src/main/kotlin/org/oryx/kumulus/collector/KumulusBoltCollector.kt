package org.oryx.kumulus.collector

import org.apache.storm.generated.Grouping
import org.apache.storm.task.IOutputCollector
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.KumulusEmitter

class KumulusBoltCollector(
        componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>>,
        emitter: KumulusEmitter
) : KumulusCollector(componentRegisteredOutputs, emitter), IOutputCollector {

    override fun emitDirect(taskId: Int, streamId: String?, anchors: MutableCollection<Tuple>?, tuple: MutableList<Any>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun resetTimeout(input: Tuple?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun fail(input: Tuple?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun reportError(error: Throwable?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun ack(input: Tuple?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun emit(streamId: String?, anchors: MutableCollection<Tuple>?, tuple: MutableList<Any>?): MutableList<Int> {
        return super.emit(streamId, tuple)
    }
}