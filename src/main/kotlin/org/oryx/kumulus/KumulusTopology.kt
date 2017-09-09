package org.oryx.kumulus

import java.util.concurrent.TimeUnit
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.ArrayBlockingQueue


class KumulusTopology(private val components: List<KumulusComponent>) {
    private val queue: ArrayBlockingQueue<Runnable> = ArrayBlockingQueue(20)
    private val ex : ThreadPoolExecutor

    init {
        ex = ThreadPoolExecutor(4, 10, 20, TimeUnit.SECONDS, queue)
    }

    fun prepare() {
        components.forEach { it.prepare() }
    }

    fun start() {
        val spouts = components.filter { it is KumulusSpout }
        val bolts = components.filter { it !is KumulusSpout }
    }
}
