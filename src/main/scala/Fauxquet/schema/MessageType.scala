package main.scala.Fauxquet.schema

import main.scala.Fauxquet.FauxquetObjs.INT32
import main.scala.Fauxquet.column.ColumnDescriptor

/**
  * Created by james on 1/28/17.
  * Used for Schema (for now)
  */
class MessageType(name: String, fields: List[BaseType]) extends GroupType(REPEATED, name, null, fields, null) {
  def getMaxRepetitionLevel(path: Array[String]): Int = getMaxRepetitionLevel(path, 0) - 1
  def getMaxDefinitionLevel(path: Array[String]): Int = getMaxDefinitionLevel(path, 0) - 1

  def getType(path: String*): BaseType = getType(path.toArray, 0)

  def getColumnDescription(path: Array[String]): ColumnDescriptor = {
    val maxR = getMaxRepetitionLevel(path)
    val maxD = getMaxDefinitionLevel(path)

    new ColumnDescriptor(path, INT32, 0, getMaxRepetitionLevel(path), getMaxDefinitionLevel(path))
  }

  override def accept(visitor: TypeVisitor): Unit = visitor.visit(this)
}