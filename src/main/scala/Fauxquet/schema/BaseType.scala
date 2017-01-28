package main.scala.Fauxquet.schema

/**
  * Created by james on 1/28/17.
  */
abstract class BaseType(val name: String, val repetition: Repetition, val originalType: OriginalType = null, val id: ID = null) {
  def withId(id: ID): BaseType
  def isRepetition(repetition: Repetition): Boolean = this.repetition == repetition
  def isPrimitive: Boolean

  def accept(visitor: TypeVisitor)

  def asGroupType: GroupType = {
    if (this.isPrimitive) throw new Error("Primitive != GroupType")
    else this.asInstanceOf[GroupType]
  }

  def equals(otherType: BaseType): Boolean = name.equals(otherType.name) &&
    repetition == otherType.repetition &&
    eqOrBothNull(repetition, otherType.repetition) &&
    eqOrBothNull(id, otherType.id)

  def eqOrBothNull(o1: Any, o2: Any): Boolean = {
    (o1 == null && o2 == null) || (o1 != null && o1.equals(o2))
  }

  def getMaxRepetitionLevel(path: Array[String], i: Int): Int
  def getMaxDefinitionLevel(path: Array[String], i: Int): Int
  def getType(path: Array[String], i: Int): BaseType
  def getPaths(depth: Int): List[Array[String]]
  def containsPath(path: Array[String], i: Int): Boolean

  def union(toMerge: BaseType): BaseType
  def union(toMerge: BaseType, strict: Boolean): BaseType

  def checkContains(sub: BaseType): Unit = {
    if (!this.name.equals(sub.name) || this.repetition != sub.repetition) { throw new Error("Error in checkContains") }
  }
}

class ID(val id: Int) {
  def intValue(): Int = id

  override def equals(o: scala.Any): Boolean = o.isInstanceOf[ID] && o.asInstanceOf[ID].id == this.id
  override def hashCode(): Int = id
  override def toString: String = String.valueOf(id)
}