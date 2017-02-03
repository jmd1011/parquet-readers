package main.scala.Fauxquet.io

import main.scala.Fauxquet.column.ColumnDescriptor
import main.scala.Fauxquet.schema.{BaseType, PrimitiveTypeName}

/**
  * Created by james on 1/28/17.
  */
class PrimitiveColumnIO(baseType: BaseType, parent: GroupColumnIO, index: Int, val id: Int) extends ColumnIO(baseType, parent, index) {
  var path: Array[ColumnIO] = _
  var columnDescriptor: ColumnDescriptor = _

  override def setLevels(r: Int, d: Int, fieldPath: Array[String], indexFieldPath: Array[Int], repetition: List[ColumnIO], path: List[ColumnIO]): Unit = {
    super.setLevels(r, d, fieldPath, indexFieldPath, repetition, path)

    val primitive = this.baseType.asPrimitiveType
    this.columnDescriptor = new ColumnDescriptor(fieldPath, primitive.primitive, primitive.typeLength, repetitionLevel, definitionLevel)
    this.path = path.toArray
  }

  override def columnNames: List[Array[String]] = List[Array[String]](this.fieldPath)

  override def getLast: PrimitiveColumnIO = this
  override def getFirst: PrimitiveColumnIO = this

  def getFirst(r: Int): PrimitiveColumnIO = getParent(r).getFirst
  def isFirst(r: Int): Boolean = getFirst(r) == this

  def primitive: PrimitiveTypeName = this.baseType.asPrimitiveType.primitive
}
