package main.scala.Parq

import org.apache.parquet.schema.PrimitiveType

import scala.collection.mutable.ListBuffer

/**
  * Created by James on 8/1/2016.
  */
class MessageType(name: String, fields: List[Type]) extends GroupType(name, REPEATED, null, fields, null) {
  override def accept(visitor: TypeVisitor) = visitor.visit(this)
  override def writeToString(string: String, indent: String): String = {
    s"${string}message $name ${if (originalType == null) "" else s"($originalType)"} {\n${membersDisplayString("  ")}}\n"
  }

  def columns: List[ColumnDescriptor] = ??? //{
//    val paths = getPaths(0)
//    var cols: ListBuffer[ColumnDescriptor] = ListBuffer[ColumnDescriptor]()
//
//    for (path: Array[String] <- paths) {
//      val primitiveType: PrimitiveType = getType(path, 0) asPrimitiveType
//
//      cols += new ColumnDescriptor(
//        path,
//        primitiveType getPrimitiveTypeName,
//        primitiveType getTypeLength,
//        getMaxRepetitionLevel(path),
//        getMaxDefinitionLevel(path))
//    }
//  }

  def getMaxRepetitionLevel(path: Array[String]): Int = ???

  def getMaxDefinitionLevel(path: Array[String]): Int = ???
}