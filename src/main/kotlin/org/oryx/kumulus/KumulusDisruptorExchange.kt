package org.oryx.kumulus

import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.RingBuffer
import com.lmax.disruptor.dsl.Disruptor
import org.apache.storm.shade.com.google.common.util.concurrent.ThreadFactoryBuilder
import java.nio.ByteBuffer

data class LongEvent(public var nanoTime: Long = 0)

class LongEventFactory : EventFactory<LongEvent> {
    override fun newInstance(): LongEvent {
        return LongEvent()
    }
}

class LongEventProducer(private val ringBuffer: RingBuffer<LongEvent>) {
    fun onData(bb: ByteBuffer) {
        val sequence = ringBuffer.next()  // Grab the next sequence
        try {
            val event = ringBuffer.get(sequence) // Get the entry in the Disruptor
            event.nanoTime = System.nanoTime()
        } finally {
            ringBuffer.publish(sequence)
        }
    }
}

object LongEventMain {
    @JvmStatic
    fun main(args: Array<String>) {
        // The factory for the event
        val factory = LongEventFactory()

        // Specify the size of the ring buffer, must be power of 2.
        val bufferSize = 1

        val threadFactory =
                ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("disruptor-thread-%d")
                        .build()!!

        // Construct the Disruptor
        val disruptor = Disruptor(factory, bufferSize, threadFactory)

        // Connect the handler
        disruptor.handleEventsWith(EventHandler<LongEvent> {
            event, sequence, endOfBatch ->
            println("TOOK: ${(System.nanoTime() - event.nanoTime) / 1000.0 / 1000.0}, event, sequence, endOfBatch = $event, $sequence, $endOfBatch")
        })

        // Start the Disruptor, starts all threads running
        disruptor.start()

        // Get the ring buffer from the Disruptor to be used for publishing.
        val ringBuffer = disruptor.ringBuffer!!

        val producer = LongEventProducer(ringBuffer)

        val bb = ByteBuffer.allocate(8)
        var l: Long = 0
        while (true) {
            bb.putLong(0, l)
            producer.onData(bb)
            Thread.sleep(1000)
            l++
        }
    }
}