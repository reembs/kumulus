package org.xyro.kumulus.component

/**
 * Implementing this interface by a spout means that its default Storm fail(Object) method will never be called. Instead,
 * the interface's fail(Any?, List<String>) would be called, with a list of timeout bolts.
 */
interface KumulusTimeoutAwareSpout {
    /**
     * Spout fail hook that includes a list of timeout bolts (bolts that did not ack/fail the msgId)
     */
    fun fail(msgId: Any?, timeoutComponents: List<String>)
}