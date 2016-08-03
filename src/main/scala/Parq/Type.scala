package main.scala.Parq

import org.apache.parquet.schema.{OriginalType, PrimitiveType}

/**
  * Created by James on 8/1/2016.
  */
abstract class Type(val name: String, val repetition: Repetition, val originalType: OriginalType, val id: ID) {
  def withId(id: Int): Type
  def isRepetition(repetition: Repetition) = this.repetition == repetition
  def isPrimitive: Boolean

  def asGroupType: GroupType = {
    if (isPrimitive) throw new Error("Not a group")
    else this.asInstanceOf[GroupType]
  }

  def asPrimitiveType: PrimitiveType = {
    if (!isPrimitive) throw new Error("Not primitive")
    else this.asInstanceOf[PrimitiveType]
  }

  def writeToString(string: String, indent: String): String

  def accept(visitor: TypeVisitor)
  def getPaths(depth: Int): List[Array[String]]
  def getType(path: Array[String], i: Int): Type

  def equals(other: Type) = this.name == other.name && this.repetition == other.repetition && this.repetition == other.repetition && this.id == other.id
  //override def equals(other: Object): Boolean = other.isInstanceOf[Type] && other != null && equals(other.asInstanceOf[Type])
}

class ID(val id: Int) {
//  override def equals(obj: Object): Boolean = {
//    obj.isInstanceOf[ID] && obj.asInstanceOf[ID].id == this.id
//  }

  override def hashCode: Int = id
  override def toString: String = id toString
}




abstract class Repetition {
  def isMoreRestrictiveThan(other: Repetition): Boolean
  def name: String
}
object REQUIRED extends Repetition {
  override def isMoreRestrictiveThan(other: Repetition): Boolean = other != REQUIRED
  override def name: String = "REQUIRED"
}
object Optional extends Repetition {
  override def isMoreRestrictiveThan(other: Repetition): Boolean = other == REPEATED
  override def name: String = "OPTIONAL"
}
object REPEATED extends Repetition {
  override def isMoreRestrictiveThan(other: Repetition): Boolean = false
  override def name: String = "REPEATED"
}