package org.oryx.kumulus.component

import clojure.lang.*
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.task.GeneralTopologyContext
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.MessageId
import org.apache.storm.tuple.Tuple

class TupleImpl : Tuple {
    val spoutMessageId: Any?

    private val context: GeneralTopologyContext
    private val values: List<Any>
    private val taskId: Int
    private val streamId: String
    private val id: MessageId

    constructor(context: GeneralTopologyContext, values: List<Any>, taskId: Int, streamId: String, id: MessageId, spoutMessageId: Any?) {
        this.context = context
        this.values = values
        this.taskId = taskId
        this.streamId = streamId
        this.id = id
        this.spoutMessageId = spoutMessageId
        val componentId = context.getComponentId(taskId)
        val schema = context.getComponentOutputFields(componentId, streamId)
        if (values.size != schema.size()) {
            throw IllegalArgumentException(
                    "Tuple created with wrong number of fields. " +
                            "Expected " + schema.size() + " fields but got " +
                            values.size + " fields")
        }
    }

    constructor(context: GeneralTopologyContext, values: List<Any>, taskId: Int, streamId: String) :
            this(context, values, taskId, streamId, MessageId.makeUnanchored(), null)


    override fun size(): Int {
        return values.size
    }

    override fun fieldIndex(field: String): Int {
        return fields.fieldIndex(field)
    }

    override operator fun contains(field: String): Boolean {
        return fields.contains(field)
    }

    override fun getValue(i: Int): Any {
        return values[i]
    }

    override fun getString(i: Int): String {
        return values[i] as String
    }

    override fun getInteger(i: Int): Int? {
        return values[i] as Int
    }

    override fun getLong(i: Int): Long? {
        return values[i] as Long
    }

    override fun getBoolean(i: Int): Boolean? {
        return values[i] as Boolean
    }

    override fun getShort(i: Int): Short? {
        return values[i] as Short
    }

    override fun getByte(i: Int): Byte? {
        return values[i] as Byte
    }

    override fun getDouble(i: Int): Double? {
        return values[i] as Double
    }

    override fun getFloat(i: Int): Float? {
        return values[i] as Float
    }

    override fun getBinary(i: Int): ByteArray {
        return values[i] as ByteArray
    }


    override fun getValueByField(field: String): Any {
        return values[fieldIndex(field)]
    }

    override fun getStringByField(field: String): String {
        return values[fieldIndex(field)] as String
    }

    override fun getIntegerByField(field: String): Int? {
        return values[fieldIndex(field)] as Int
    }

    override fun getLongByField(field: String): Long? {
        return values[fieldIndex(field)] as Long
    }

    override fun getBooleanByField(field: String): Boolean? {
        return values[fieldIndex(field)] as Boolean
    }

    override fun getShortByField(field: String): Short? {
        return values[fieldIndex(field)] as Short
    }

    override fun getByteByField(field: String): Byte? {
        return values[fieldIndex(field)] as Byte
    }

    override fun getDoubleByField(field: String): Double? {
        return values[fieldIndex(field)] as Double
    }

    override fun getFloatByField(field: String): Float? {
        return values[fieldIndex(field)] as Float
    }

    override fun getBinaryByField(field: String): ByteArray {
        return values[fieldIndex(field)] as ByteArray
    }

    override fun getValues(): List<Any> {
        return values
    }

    override fun getFields(): Fields {
        return context.getComponentOutputFields(sourceComponent, sourceStreamId)
    }

    override fun select(selector: Fields): List<Any> {
        return fields.select(selector, values)
    }

    @Deprecated("", ReplaceWith("sourceGlobalStreamId"))
    override fun getSourceGlobalStreamid(): GlobalStreamId {
        return sourceGlobalStreamId
    }

    override fun getSourceGlobalStreamId(): GlobalStreamId {
        return GlobalStreamId(sourceComponent, streamId)
    }

    override fun getSourceComponent(): String {
        return context.getComponentId(taskId)
    }

    override fun getSourceTask(): Int {
        return taskId
    }

    override fun getSourceStreamId(): String {
        return streamId
    }

    override fun getMessageId(): MessageId {
        return id
    }

    override fun toString(): String {
        return """source: $sourceComponent:$taskId, stream: $streamId, id: $id, $values"""
    }

    override fun equals(other: Any?): Boolean {
        return this === other
    }

    override fun hashCode(): Int {
        return System.identityHashCode(this)
    }
}