package main.scala.Parq

import org.apache.parquet.schema.OriginalType

import scala.collection.mutable.ListBuffer

/**
  * Created by James on 8/1/2016.
  */
class GroupType(name: String, repetition: Repetition, originalType: OriginalType, val fields: List[Type], id: ID)
    extends Type(name, repetition, originalType, id) {

  val indexByName: Map[String, Int] = {
    def idx(f: List[Type], acc: Map[String, Int], i: Int): Map[String, Int] = f match {
      case List() => acc
      case x :: xs => acc.+((x.name, i))
    }

    idx(fields, Map[String, Int](), 0)
  }

  override def withId(id: Int): Type = new GroupType(name, repetition, originalType, fields, new ID(id))

  override def isPrimitive: Boolean = false

  override def writeToString(string: String, indent: String): String =
    string +
      indent +
      repetition.name.toLowerCase() +
      " group " +
      name +
      (
        if (originalType == null) ""
        else s"($originalType)"
        ) +
      (
        if (id == null) ""
        else " = " + id
        ) +
      " {\n" +
      membersDisplayString(indent) +
      indent +
      "}"

  def membersDisplayString(indent: String): String = {
    def membersDisplayString0(indent: String, acc: String, f: List[Type]): String = f match {
      case List() => ""
      case x :: xs => membersDisplayString0(indent, x.writeToString(acc, indent), xs)
    }

    membersDisplayString0(indent, "", fields)
  }

  override def accept(visitor: TypeVisitor): Unit = visitor.visit(this)

  def getPaths(depth: Int): List[Array[String]] = {
    var res: ListBuffer[Array[String]] = ListBuffer[Array[String]]()

    for (field: Type <- fields) {
      val paths = field getPaths (depth + 1)

      for (path: Array[String] <- paths) {
        path(depth) = field name

        res += path
      }
    }

    res toList
  }

  def getType(fieldName: String): Type = getType(getFieldIndex(fieldName))
  def getType(index: Int) = fields(index)
  override def getType(path: Array[String], depth: Int): Type = if (depth == path.length) this else getType(path(depth)).getType(path, depth + 1)

  def getFieldIndex(name: String): Int = {
    if (!indexByName.contains(name)) {
      throw new Error(s"$name not found in $this")
    }

    indexByName(name)
  }
}