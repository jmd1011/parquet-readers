package main.scala.Fauxquet.schema

import main.scala.Fauxquet.column.ColumnDescriptor

/**
  * Created by james on 1/28/17.
  * Used for Schema (for now)
  */
class MessageType(name: String, fields: List[BaseType]) extends GroupType(REPEATED, name, null, fields, null) {
  def getMaxRepetitionLevel(path: Array[String]): Int = getMaxRepetitionLevel(path, 0) - 1
  def getMaxDefinitionLevel(path: Array[String]): Int = getMaxDefinitionLevel(path, 0) - 1

  def getType(path: String*): BaseType = getType(path.toArray)
  def getType(path: Array[String]): BaseType = getType(path, 0)

  def getColumnDescription(path: Array[String]): ColumnDescriptor = {
    val maxR = getMaxRepetitionLevel(path)
    val maxD = getMaxDefinitionLevel(path)

    val primitiveType = getType(path, 0).asPrimitiveType

    new ColumnDescriptor(path, primitiveType.primitive, 0, getMaxRepetitionLevel(path), getMaxDefinitionLevel(path))
  }

  def columns(): List[ColumnDescriptor] = {
    val paths = this.getPaths(0)
    var cols = List[ColumnDescriptor]()

    for (path <- paths) {
      val primitiveType = getType(path, 0).asPrimitiveType

      cols ::= new ColumnDescriptor(path, primitiveType.primitive, primitiveType.typeLength, getMaxRepetitionLevel(path), getMaxDefinitionLevel(path))
    }

    cols
  }

  override def accept(visitor: TypeVisitor): Unit = visitor.visit(this)
}