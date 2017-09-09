package org.oryx.kumulus

import org.apache.storm.task.IOutputCollector
import org.apache.storm.tuple.Tuple

class KumulusBoltCollector : KumulusCollector(), IOutputCollector {
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
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}