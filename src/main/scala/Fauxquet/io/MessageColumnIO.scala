package main.scala.Fauxquet.io

import main.scala.Fauxquet.column.ColumnWriteStore
import main.scala.Fauxquet.column.ColumnWriters.ColumnWriter
import main.scala.Fauxquet.io.api.RecordConsumer
import main.scala.Fauxquet.schema.MessageType

import scala.collection.immutable.BitSet.BitSet1
import scala.collection.mutable

/**
  * Created by james on 1/28/17.
  */
class MessageColumnIO(messageType: MessageType, val validating: Boolean, val createdBy: String) extends GroupColumnIO(messageType, null, 0) {
  var leaves: List[PrimitiveColumnIO] = _

  /**
    *
    * RecordReader nonsense here for reading records -- hopefully this won't be required (it shouldn't be)
    *
    */

  /**
    * Writing records
    * @param columns
    */
  class MessageColumnIORecordConsumer(val columns: ColumnWriteStore) extends RecordConsumer {
    class FieldsMarker {
      var visitedIndexes = new java.util.BitSet()

      def reset(fieldsCount: Int) = this.visitedIndexes.clear(0, fieldsCount)
      def markWritten(i: Int) = this.visitedIndexes.set(i)
      def isWritten(i: Int): Boolean = this.visitedIndexes.get(i)
    }

    var fieldsWritten: Array[FieldsMarker] = _
    var r: Array[Int] = _
    val columnWriter = new Array[ColumnWriter](leaves.size) //TODO: I think this can be simplified to having just one ColumnWriter for us

    var groupToLeafWriter: Map[GroupColumnIO, List[ColumnWriter]] = Map[GroupColumnIO, List[ColumnWriter]]()

    def buildGroupToLeafWritersMap(primitiveColumnIO: PrimitiveColumnIO, writer: ColumnWriter): Unit = {
      var parent = primitiveColumnIO.parent

      do {
        getLeafWriters(parent) ::= writer
        parent = parent.parent
      } while (parent != null)
    }

    def getLeafWriters(groupColumnIO: GroupColumnIO): List[ColumnWriter] = {
      var writers = groupToLeafWriter(groupColumnIO)

      if (writers == null) {
        writers = List[ColumnWriter]()
        groupToLeafWriter += (groupColumnIO -> writers)
      }
    }

    def init() = {
      var maxDepth = 0

      for (primColIO <- leaves) {
        val w = columns.getColumnWriter(primColIO.columnDescriptor)
        maxDepth = math.max(maxDepth, primColIO.fieldPath.length)
        columnWriter(primColIO.id) = w
        buildGroupToLeafWritersMap(primColIO, w)
      }

      fieldsWritten = new Array[FieldsMarker](maxDepth)

      for (i <- 0 until maxDepth) {
        fieldsWritten(i) = new FieldsMarker()
      }

      r = new Array[Int](maxDepth)
    }
    init()
  }

  def getRecordWriter(columns: ColumnWriteStore): RecordConsumer = {
    new MessageColumnIORecordConsumer(null)
  }
}