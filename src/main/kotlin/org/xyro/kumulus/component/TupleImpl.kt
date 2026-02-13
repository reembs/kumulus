package org.xyro.kumulus.component

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.task.GeneralTopologyContext
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.MessageId
import org.apache.storm.tuple.Tuple

open class TupleImpl : Tuple {
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
                    values.size + " fields",
            )
        }
    }

    constructor(context: GeneralTopologyContext, values: List<Any>, taskId: Int, streamId: String) :
        this(context, values, taskId, streamId, MessageId.makeUnanchored(), null)

    override fun size(): Int = values.size

    override fun fieldIndex(field: String): Int = fields.fieldIndex(field)

    override operator fun contains(field: String): Boolean = fields.contains(field)

    override fun getValue(i: Int): Any = values[i]

    override fun getString(i: Int): String = values[i] as String

    override fun getInteger(i: Int): Int? = values[i] as Int

    override fun getLong(i: Int): Long? = values[i] as Long

    override fun getBoolean(i: Int): Boolean? = values[i] as Boolean

    override fun getShort(i: Int): Short? = values[i] as Short

    override fun getByte(i: Int): Byte? = values[i] as Byte

    override fun getDouble(i: Int): Double? = values[i] as Double

    override fun getFloat(i: Int): Float? = values[i] as Float

    override fun getBinary(i: Int): ByteArray = values[i] as ByteArray

    override fun getValueByField(field: String): Any = values[fieldIndex(field)]

    override fun getStringByField(field: String): String = values[fieldIndex(field)] as String

    override fun getIntegerByField(field: String): Int? = values[fieldIndex(field)] as Int

    override fun getLongByField(field: String): Long? = values[fieldIndex(field)] as Long

    override fun getBooleanByField(field: String): Boolean? = values[fieldIndex(field)] as Boolean

    override fun getShortByField(field: String): Short? = values[fieldIndex(field)] as Short

    override fun getByteByField(field: String): Byte? = values[fieldIndex(field)] as Byte

    override fun getDoubleByField(field: String): Double? = values[fieldIndex(field)] as Double

    override fun getFloatByField(field: String): Float? = values[fieldIndex(field)] as Float

    override fun getBinaryByField(field: String): ByteArray = values[fieldIndex(field)] as ByteArray

    override fun getValues(): List<Any> = values

    override fun getFields(): Fields = context.getComponentOutputFields(sourceComponent, sourceStreamId)

    override fun select(selector: Fields): List<Any> = fields.select(selector, values)

    @Deprecated("", ReplaceWith("sourceGlobalStreamId"))
    override fun getSourceGlobalStreamid(): GlobalStreamId = sourceGlobalStreamId

    override fun getSourceGlobalStreamId(): GlobalStreamId = GlobalStreamId(sourceComponent, streamId)

    override fun getSourceComponent(): String = context.getComponentId(taskId)

    override fun getSourceTask(): Int = taskId

    override fun getSourceStreamId(): String = streamId

    override fun getMessageId(): MessageId = id

    override fun toString(): String =
        """source: $sourceComponent:$taskId, stream: $streamId, id: $id, $values [spoutMessageId: $spoutMessageId]"""

    override fun equals(other: Any?): Boolean = this === other

    override fun hashCode(): Int = System.identityHashCode(this)
}
