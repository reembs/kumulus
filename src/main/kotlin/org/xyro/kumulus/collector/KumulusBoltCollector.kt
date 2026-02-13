package org.xyro.kumulus.collector

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.storm.task.IOutputCollector
import org.apache.storm.tuple.Tuple
import org.xyro.kumulus.KumulusAcker
import org.xyro.kumulus.KumulusEmitter
import org.xyro.kumulus.component.KumulusBolt
import org.xyro.kumulus.component.KumulusComponent

class KumulusBoltCollector(
    component: KumulusComponent,
    private val emitter: KumulusEmitter,
    acker: KumulusAcker,
    errorHandler: ((String, Int, Throwable) -> Unit)?,
) : KumulusCollector<KumulusBolt>(
        component,
        emitter,
        acker,
        errorHandler,
    ),
    IOutputCollector {
    companion object {
        val logger = KotlinLogging.logger { }
    }

    override fun emitDirect(
        taskId: Int,
        streamId: String?,
        anchors: MutableCollection<Tuple>?,
        tuple: MutableList<Any>?,
    ) {
        TODO("not implemented")
    }

    override fun resetTimeout(input: Tuple?) {
        TODO("not implemented")
    }

    override fun fail(input: Tuple?) {
        try {
            acker.fail(component, input)
        } catch (t: Throwable) {
            emitter.throwException(t)
        }
    }

    override fun ack(input: Tuple?) {
        try {
            acker.ack(component, input)
        } catch (t: Throwable) {
            emitter.throwException(t)
        }
    }
}
