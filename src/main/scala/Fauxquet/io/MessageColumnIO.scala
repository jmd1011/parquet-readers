package main.scala.Fauxquet.io

import main.scala.Fauxquet.column.ColumnWriteStore
import main.scala.Fauxquet.column.ColumnWriters.ColumnWriter
import main.scala.Fauxquet.io.api.{Binary, RecordConsumer}
import main.scala.Fauxquet.schema.MessageType

/**
  * Created by james on 1/28/17.
  */
class MessageColumnIO(messageType: MessageType, val validating: Boolean, val createdBy: String) extends GroupColumnIO(messageType, null, 0) {
  val leaves: List[PrimitiveColumnIO] = {
    var l: List[PrimitiveColumnIO] = List[PrimitiveColumnIO]()

    for (i <- messageType.fields.indices) {
      val f = messageType.fields(i)

      l ::= new PrimitiveColumnIO(f, this, i, i)
    }

    l
  }

  for (leaf <- leaves) leaf.setLevels(0, 0, Array[String](leaf.name), Array[Int](), List[ColumnIO](this), List[ColumnIO](this))

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

    var currentColumnIO: ColumnIO = _
    var currentLevel: Int = 0
    var emptyField: Boolean = true

    val columnWriter = new Array[ColumnWriter](leaves.size)

    var groupToLeafWriter: Map[GroupColumnIO, List[ColumnWriter]] = Map[GroupColumnIO, List[ColumnWriter]]()
    var groupNullCache: Map[GroupColumnIO, List[Int]] = Map[GroupColumnIO, List[Int]]()

    def buildGroupToLeafWritersMap(primitiveColumnIO: PrimitiveColumnIO, writer: ColumnWriter): Unit = {
      var parent = primitiveColumnIO.parent //TODO: Need parent to be "m"

      do {
        var w = getLeafWriters(parent)
        w ::= writer
        //getLeafWriters(parent) ::= writer
        parent = parent.parent
      } while (parent != null)
    }

    def getLeafWriters(groupColumnIO: GroupColumnIO): List[ColumnWriter] = {
      val writers = groupToLeafWriter.get(groupColumnIO)

      writers match {
        case None =>
          val x = List[ColumnWriter]()
          groupToLeafWriter += (groupColumnIO -> x)
          x
        case Some(x) => x
      }
    }

    def curColumnWriter = columnWriter(currentColumnIO.asInstanceOf[PrimitiveColumnIO].id)

    override def addInteger(int: Int): Unit = {
      emptyField = false
      curColumnWriter.write(int, r(currentLevel), currentColumnIO.definitionLevel)

      setRepetitionLevels()
    }

    override def addLong(long: Long): Unit = {
      emptyField = false
      curColumnWriter.write(long, r(currentLevel), currentColumnIO.definitionLevel)

      setRepetitionLevels()
    }

    override def addBool(boolean: Boolean): Unit = {
      emptyField = false
      curColumnWriter.write(boolean, r(currentLevel), currentColumnIO.definitionLevel)

      setRepetitionLevels()
    }

    override def addFloat(float: Float): Unit = {
      emptyField = false
      curColumnWriter.write(float, r(currentLevel), currentColumnIO.definitionLevel)

      setRepetitionLevels()
    }

    override def addDouble(double: Double): Unit = {
      emptyField = false
      curColumnWriter.write(double, r(currentLevel), currentColumnIO.definitionLevel)

      setRepetitionLevels()
    }

    override def flush(): Unit = {
      flushCachedNulls(MessageColumnIO.this)
    }

    override def startMessage(): Unit = {
      currentColumnIO = MessageColumnIO.this
      r(0) = 0

      val numberOfFieldsToVisit = currentColumnIO.asInstanceOf[GroupColumnIO].childrenSize
      fieldsWritten(0).reset(numberOfFieldsToVisit)
    }

    override def endMessage(): Unit = {
      writeNullForMissingFieldsAtCurrentLevel()
      columns.endRecord()
    }

    override def startField(field: String, index: Int): Unit = {
      currentColumnIO = currentColumnIO.asInstanceOf[GroupColumnIO].getChild(index)
      emptyField = true
    }

    override def endField(field: String, index: Int): Unit = {
      currentColumnIO = currentColumnIO.parent

      if (emptyField) throw new Error("Empty fields are illegal, like everything else in this country")

      fieldsWritten(currentLevel).markWritten(index)
      r(currentLevel) = if (currentLevel == 0) 0 else r(currentLevel - 1)
    }

    def setRepetitionLevels(): Unit = r(currentLevel) = currentColumnIO.repetitionLevel

    override def startGroup(): Unit = {
      val group = currentColumnIO.asInstanceOf[GroupColumnIO]

      if (hasNullCache(group)) {
        flushCachedNulls(group)
      }

      currentLevel += 1
      r(currentLevel) = r(currentLevel - 1)

      val fieldsCount = currentColumnIO.asInstanceOf[GroupColumnIO].childrenSize
      fieldsWritten(currentLevel).reset(fieldsCount)
    }

    def hasNullCache(groupColumnIO: GroupColumnIO): Boolean = {
      val nulls = groupNullCache(groupColumnIO)

      nulls != null && nulls.nonEmpty
    }

    def flushCachedNulls(groupColumnIO: GroupColumnIO): Unit = {
      for (i <- 0 until groupColumnIO.childrenSize) {
        val child = groupColumnIO.getChild(i)

        //TODO: Is this actually faster/better?
        child match {
          case o: GroupColumnIO =>
            flushCachedNulls(o)
          case _ =>
        }
      }

      writeNullToLeaves(groupColumnIO)
    }

    def writeNullToLeaves(groupColumnIO: GroupColumnIO): Unit = {
      var nulls = groupNullCache(groupColumnIO)

      if (nulls == null || nulls.isEmpty) return

      val parentDLevel = groupColumnIO.parent.definitionLevel

      for (leafWriter <- groupToLeafWriter(groupColumnIO)) {
        for (int <- nulls) {
          leafWriter.writeNull(int, parentDLevel)
        }
      }

      nulls = List[Int]()
    }

    override def endGroup(): Unit = {
      emptyField = false
      writeNullForMissingFieldsAtCurrentLevel()
      currentLevel -= 1

      setRepetitionLevels()
    }

    def writeNullForMissingFieldsAtCurrentLevel(): Unit = {
      val currentFieldsCount = currentColumnIO.asInstanceOf[GroupColumnIO].childrenSize


      for (i <- 0 until currentFieldsCount) {
        if (!fieldsWritten(currentLevel).isWritten(i)) {
          val undefinedField = currentColumnIO.asInstanceOf[GroupColumnIO].getChild(i)
          val d = currentColumnIO.definitionLevel

          writeNull(undefinedField, r(currentLevel), d)
        }
      }
    }

    def writeNull(undefinedField: ColumnIO, r: Int, d: Int): Unit = {
      if (undefinedField.baseType.isPrimitive) {
        columnWriter(undefinedField.asInstanceOf[PrimitiveColumnIO].id).writeNull(r, d)
      }
      else {
        cacheNullForGroup(undefinedField.asInstanceOf[GroupColumnIO], r)
      }
    }

    def cacheNullForGroup(groupColumnIO: GroupColumnIO, r: Int): Unit = {
      var nulls = groupNullCache(groupColumnIO)

      if (nulls == null) {
        nulls = List[Int]()
        groupNullCache += (groupColumnIO -> nulls)
      }

      nulls ::= r
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

    def getColumnWriter: ColumnWriter = {
      columnWriter(currentColumnIO.asInstanceOf[PrimitiveColumnIO].id)
    }

    override def addBinary(binary: Binary): Unit = {
      emptyField = false
      getColumnWriter.write(binary, r(currentLevel), currentColumnIO.definitionLevel)
    }
  }

  def setLevels(): Unit = {
    setLevels(0, 0, new Array[String](0), new Array[Int](0), List[ColumnIO](this), List[ColumnIO](this))
  }

  def getRecordWriter(columns: ColumnWriteStore): RecordConsumer = {
    new MessageColumnIORecordConsumer(columns) //TODO: Something about validating here, but seems to always be false
  }
}
