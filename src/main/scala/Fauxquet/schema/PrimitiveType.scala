package main.scala.Fauxquet.schema

import main.scala.Fauxquet.schema.OriginalType.OriginalType

/**
  * Created by james on 1/28/17.
  */
class PrimitiveType(repetition: Repetition, val typeLength: Int, name: String, originalType: OriginalType, val decimalMetadata: DecimalMetadata, id: ID)
  extends BaseType(name, repetition, originalType, id) {
  override def withId(id: ID): BaseType = new PrimitiveType(repetition, typeLength, name, originalType, decimalMetadata, id)

  override def isPrimitive: Boolean = true

  override def getMaxRepetitionLevel(path: Array[String], i: Int): Int =
    if (path.length != i) throw new Error("Stuff happens")
    else if (isRepetition(REPEATED)) 1 else 0

  override def getMaxDefinitionLevel(path: Array[String], i: Int): Int =
    if (path.length != i) throw new Error("Stuff happens")
    else if (isRepetition(REQUIRED)) 0 else 1

  override def getType(path: Array[String], i: Int): BaseType = if (path.length != i) throw new Error("Ick") else this

  override def getPaths(depth: Int): List[Array[String]] = List[Array[String]](new Array[String](depth))

  override def containsPath(path: Array[String], i: Int): Boolean = path.length == i

  override def union(toMerge: BaseType): BaseType = union(toMerge, strict = true)

  override def union(toMerge: BaseType, strict: Boolean): BaseType = {
    if (!toMerge.isPrimitive) {
      throw new Error("Can only merge primitives")
    }

    throw new Error("Shouldn't call union apparently")
  }

  override def accept(visitor: TypeVisitor): Unit = visitor.visit(this)
}