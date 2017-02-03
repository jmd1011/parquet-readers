package main.scala.Fauxquet.schema

import main.scala.Fauxquet.schema.OriginalType.OriginalType

/**
  * Created by james on 1/28/17.
  */
class GroupType(repetition: Repetition, name: String, originalType: OriginalType = null, val fields: List[BaseType], id: ID = null) extends BaseType(name, repetition, originalType, id) {
  val indexByName = fields.map(x => (x.name, fields.indexOf(x))).toMap

  override def withId(id: ID): BaseType = new GroupType(this.repetition, this.name, this.originalType, this.fields, id)

  def withNewFields(fields: List[BaseType]): GroupType = new GroupType(this.repetition, this.name, this.originalType, fields, this.id)
  def withNewFields(fields: BaseType*): GroupType = withNewFields(fields.toList)

  def getFieldName(i: Int): String = indexByName.filter(_._2 == i).head._1 //TODO: there's gotta be a better solution
  def getFieldIndex(name: String): Int = indexByName(name)

  def fieldCount = this.fields.size

  override def isPrimitive: Boolean = false

  def getType(i: Int): BaseType = fields(i)

  def getType(fieldName: String): BaseType = {
    getType(getFieldIndex(fieldName))
  }

  override def getMaxRepetitionLevel(path: Array[String], i: Int): Int = {
    val r = if (isRepetition(REPEATED)) 1 else 0

    if (i == path.length) r
    else r + getType(path(i)).getMaxRepetitionLevel(path, i + 1)
  }

  override def getMaxDefinitionLevel(path: Array[String], i: Int): Int = {
    val d = if (isRepetition(REQUIRED)) 1 else 0

    if (i == path.length) d
    else d + getType(path(d)).getMaxDefinitionLevel(path, i + 1)
  }

  override def getType(path: Array[String], i: Int): BaseType =
    if (i == path.length) this
    else getType(path(i)).getType(path, i + 1)

  override def getPaths(depth: Int): List[Array[String]] = {
    var l = List[Array[String]]()

    for (field <- fields) {
      val paths = field.getPaths(depth + 1)

      for (path <- paths) {
        path(depth) = field.name
        l :+= path
      }
    }

    l
  }

  def containsField(name: String) = indexByName.contains(name)

  override def containsPath(path: Array[String], i: Int): Boolean =
    if (i == path.length) false
    else containsField(path(i)) && getType(path(i)).containsPath(path, i + 1)

  override def union(toMerge: BaseType): BaseType = union(toMerge, strict = true)

  override def union(toMerge: BaseType, strict: Boolean): BaseType =
    if (toMerge.isPrimitive) throw new Error("Bad juju")
    else new GroupType(toMerge.repetition, name, null, mergeFields(toMerge.asGroupType), null)

  def mergeFields(toMerge: GroupType): List[BaseType] = mergeFields(toMerge, strict = true)

  def mergeFields(toMerge: GroupType, strict: Boolean): List[BaseType] = {
    var newFields = List[BaseType]()

    for (field <- this.fields) {
      var merged: BaseType = null

      if (toMerge.containsField(field.name)) {
        val fieldToMerge = toMerge.getType(field.name)

        if (fieldToMerge.repetition.isMoreRestrictiveThan(field.repetition)) throw new Error("Can't merge these")

        merged = field.union(fieldToMerge, strict)
      }
      else {
        merged = field
      }

      newFields :+= merged
    }

    for (field <- toMerge.fields) {
      if (!this.containsField(field.name)) {
        newFields :+= field
      }
    }

    newFields
  }

  override def hashCode(): Int = super.hashCode() * 31 + fields.hashCode()

  override def equals(otherType: BaseType): Boolean = !otherType.isPrimitive && super.equals(otherType) && fields.equals(otherType.asGroupType.fields)

  override def accept(visitor: TypeVisitor): Unit = visitor.visit(this)
}