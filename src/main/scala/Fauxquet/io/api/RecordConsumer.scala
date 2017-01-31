package main.scala.Fauxquet.io.api

/**
  * Created by james on 1/28/17.
  *
  * * Abstraction for writing records
  * It decouples the striping algorithm from the actual record model
  * example:
  * <pre>
  * startMessage()
  *  startField("A", 0)
  *   addValue(1)
  *   addValue(2)
  *  endField("A", 0)
  *  startField("B", 1)
  *   startGroup()
  *    startField("C", 0)
  *     addValue(3)
  *    endField("C", 0)
  *   endGroup()
  *  endField("B", 1)
  * endMessage()
  * </pre>
  *
  * would produce the following message:
  * <pre>
  * {
  *   A: [1, 2]
  *   B: {
  *     C: 3
  *   }
  * }
  * </pre>
  * - Parquet
  */
abstract class RecordConsumer {
  def startMessage(): Unit
  def endMessage(): Unit

  def startField(field: String, index: Int): Unit
  def endField(field: String, index: Int): Unit

  def startGroup(): Unit
  def endGroup(): Unit

  def addInteger(int: Int): Unit
  def addLong(long: Long): Unit
  def addBool(boolean: Boolean): Unit
  def addFloat(float: Float): Unit
  def addDouble(double: Double): Unit
  def addBinary(binary: Binary): Unit

  def flush(): Unit = { }
}
