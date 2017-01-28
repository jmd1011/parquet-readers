package main.scala.Fauxquet.io

import main.scala.Fauxquet.Types.{REPEATED, REQUIRED}
import main.scala.Fauxquet.schema.{GroupType, REPEATED, REQUIRED}

/**
  * Created by james on 1/28/17.
  */
class GroupColumnIO(groupType: GroupType, parent: GroupColumnIO, index: Int) extends ColumnIO(groupType, parent, index) {
  var childrenByName = Map[String, ColumnIO]()
  var children = List[ColumnIO]()
  var childrenSize = 0 //for SPEED

  def add(columnIO: ColumnIO): Unit = {
    childrenByName += (columnIO.name -> columnIO)
    children ::= columnIO
    childrenSize += 1
  }

  override def setLevels(r: Int, d: Int, fieldPath: Array[String], indexFieldPath: Array[Int], repetition: List[ColumnIO], path: List[ColumnIO]): Unit = {
    super.setLevels(r, d, fieldPath, indexFieldPath, repetition, path)

    for (child <- this.children) {
      val newFieldPath = Array[String]()
      Array.copy(fieldPath, 0, newFieldPath, 0, fieldPath.length + 1)

      val newIndexFieldPath = Array[Int]()
      Array.copy(indexFieldPath, 0, newIndexFieldPath, 0, indexFieldPath.length + 1)

      newFieldPath(fieldPath.length) = child.baseType.name
      newIndexFieldPath(indexFieldPath.length) = child.index

      val newRepetition: List[ColumnIO] = {
        if (child.baseType.isRepetition(REPEATED)) {
          repetition.::(child) //TODO: Why this notation?
        } else {
          repetition
        }
      }

      val newPath = path.::(child)
      child.setLevels(
        if (child.baseType.isRepetition(REPEATED)) r + 1 else r,
        if (!child.baseType.isRepetition(REQUIRED)) d + 1 else d,
        newFieldPath,
        newIndexFieldPath,
        newRepetition,
        newPath
      )
    }
  }

  def getChild(name: String) = childrenByName(name)
  def getChild(i: Int) = children(i)

  override def columnNames: List[Array[String]] = children.flatMap(_.columnNames) //TODO: check this with Greg

  override def getLast: PrimitiveColumnIO = children(childrenSize - 1).getLast
  override def getFirst: PrimitiveColumnIO = children.head.getFirst
}
