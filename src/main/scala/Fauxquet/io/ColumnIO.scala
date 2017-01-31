package main.scala.Fauxquet.io

import main.scala.Fauxquet.schema.{BaseType, REPEATED}

/**
  * Created by james on 1/28/17.
  */
abstract class ColumnIO(val baseType: BaseType, val parent: GroupColumnIO, val index: Int) {
  val name = baseType.name

  var repetitionLevel: Int = 0
  var definitionLevel: Int = 0

  var fieldPath: Array[String] = Array[String]()
  var indexFieldPath: Array[Int] = Array[Int]()

  def setFieldPath(fieldPath: Array[String], indexFieldPath: Array[Int]): Unit = {
    this.fieldPath = fieldPath
    this.indexFieldPath = indexFieldPath
  }

  def setLevels(r: Int, d: Int, fieldPath: Array[String], indexFieldPath: Array[Int], repetition: List[ColumnIO], path: List[ColumnIO]): Unit = {
    repetitionLevel = r
    definitionLevel = d
    setFieldPath(fieldPath, indexFieldPath)
  }

  def columnNames: List[Array[String]]

  def getLast: PrimitiveColumnIO
  def getFirst: PrimitiveColumnIO

  def getParent(r: Int): ColumnIO = {
    if (repetitionLevel == r && baseType.isRepetition(REPEATED)) {
      this
    }
    else if (parent != null && parent.definitionLevel >= r) {
      parent.getParent(r)
    } else {
      throw new Error(s"No parent $r")
    }
  }
}
