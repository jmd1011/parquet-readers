package main.scala.Fauxquet.io

import main.scala.Fauxquet.FauxquetObjs.ColumnDescriptor
import main.scala.Fauxquet.schema.BaseType

/**
  * Created by james on 1/28/17.
  */
class PrimitiveColumnIO(baseType: BaseType, parent: GroupColumnIO, index: Int, val id: Int) extends ColumnIO(baseType, parent, index) {
  var path: Array[ColumnIO] = _
  var columnDescriptor: ColumnDescriptor = _

  override def setLevels(r: Int, d: Int, fieldPath: Array[String], indexFieldPath: Array[Int], repetition: List[ColumnIO], path: List[ColumnIO]): Unit = {
    super.setLevels(r, d, fieldPath, indexFieldPath, repetition, path)

    throw new Error("You'll need to make a PrimitiveTypeName")
    //val t = baseType.asP
  }


  override def columnNames: List[Array[String]] = ???

  override def getLast: PrimitiveColumnIO = ???

  override def getFirst: PrimitiveColumnIO = ???
}
