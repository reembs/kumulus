package org.xyro.kumulus

import java.util.concurrent.Executor
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * [Executor] decorator that tracks how many submitted tasks have not started yet, and the peak of that count.
 */
class ExecutionPool(
    private val delegate: Executor,
) : Executor {
    private val queueSize = AtomicInteger(0)
    private val maxSize = AtomicInteger(0)

    /** Peak number of submitted-but-not-started tasks since construction or the last [resetMaxQueueSize]. */
    val maxQueueSize: Int
        get() = maxSize.get()

    fun resetMaxQueueSize() {
        maxSize.set(0)
    }

    override fun execute(command: Runnable) {
        val currentSize = queueSize.incrementAndGet()
        maxSize.getAndUpdate {
            Math.max(it, currentSize)
        }
        try {
            delegate.execute {
                queueSize.decrementAndGet()
                command.run()
            }
        } catch (e: Exception) {
            queueSize.decrementAndGet()
            throw e
        }
    }

    companion object {
        /**
         * Fixed pool of [size] daemon threads named "KumulusThread-N".
         */
        fun defaultExecutor(size: Int): ThreadPoolExecutor {
            // TODO: replace with Thread.ofPlatform().name("KumulusThread-", 0).daemon().factory() once Java 21 is the minimum runtime
            val threadIndex = AtomicInteger(0)
            val threadFactory =
                ThreadFactory { runnable ->
                    Thread(runnable, "KumulusThread-${threadIndex.getAndIncrement()}").apply {
                        isDaemon = true
                    }
                }
            return ThreadPoolExecutor(
                size,
                size,
                0L,
                TimeUnit.MILLISECONDS,
                // uncapped, memory for in-flight tuples should be taken into account and factored into max-spout-pending.
                // Keep this a LinkedBlockingQueue: with a zero bolt pushback wait, a message for a busy component is
                // re-enqueued immediately, and the queue's locks are what keep that retry loop from spinning every
                // pool thread. A lock-free queue (LinkedTransferQueue) lets the retries starve the busy component's
                // own thread, and throughput collapses under load.
                LinkedBlockingQueue(),
                threadFactory,
            ).apply {
                prestartAllCoreThreads()
            }
        }
    }
}
