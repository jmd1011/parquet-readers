package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._

/**
  * Created by james on 8/9/16.
  */
class SchemaElement(parent: SchemaElement = null) extends Fauxquetable {
  var name: String = _
  var numChildren: Int = -1
  var scale: Int = -1
  var precision: Int = -1
  var fieldId: Int = -1
  var definition: Int = 0
  var repetition: Int = 0

  var children: Vector[SchemaElement] = Vector[SchemaElement]()

  var Type: TType = _
  var typeLength: Int = -1

  var fieldRepetitionType: FieldRepetitionType = _
  var convertedType: ConvertedType = _

  private val TYPE_FIELD_DESC = TField("type", 8, 1)
  private val TYPE_LENGTH_FIELD_DESC = TField("type_length", 8, 2)
  private val REPETITION_TYPE_FIELD_DESC = TField("repetition_type", 8, 3)
  private val NAME_FIELD_DESC = TField("name", 11, 4)
  private val NUM_CHILDREN_FIELD_DESC = TField("num_children", 8, 5)
  private val CONVERTED_TYPE_FIELD_DESC = TField("converted_type", 8, 6)
  private val SCALE_FIELD_DESC = TField("scale", 8, 7)
  private val PRECISION_FIELD_DESC = TField("precision", 8, 8)
  private val FIELD_ID_FIELD_DESC = TField("field_id", 8, 9)

  if (parent != null) { repetition = parent.repetition; definition = parent.definition }

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => Type = TTypeManager getType(FauxquetDecoder readI32 arr)
      case 2 => typeLength = FauxquetDecoder readI32 arr
      case 3 =>
        fieldRepetitionType = FieldRepetitionTypeManager getFieldRepetitionTypeById(FauxquetDecoder readI32 arr)

        fieldRepetitionType match {
          case OPTIONAL => definition = if (parent != null) parent.definition + 1 else 1
          case REPEATED =>
            repetition = if (parent != null) parent.repetition + 1 else 1
            definition = if (parent != null) parent.definition + 1 else 1
          case _ =>
        }
      case 5 => numChildren = FauxquetDecoder readI32 arr
      case 6 => convertedType = ConvertedTypeManager getConvertedTypeById(FauxquetDecoder readI32 arr)
      case 7 => scale = FauxquetDecoder readI32 arr
      case 8 => precision = FauxquetDecoder readI32 arr
      case 9 => fieldId = FauxquetDecoder readI32 arr
    }
    case TField(_, 11, 4) => name = FauxquetDecoder readString arr
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def doWrite(): Unit = {
    def writeType(): Unit = {
      FauxquetEncoder writeFieldBegin TYPE_FIELD_DESC
      FauxquetEncoder writeI32 Type.id
      FauxquetEncoder writeFieldEnd()
    }

    def writeTypeLength(): Unit = {
      FauxquetEncoder writeFieldBegin TYPE_LENGTH_FIELD_DESC
      FauxquetEncoder writeI32 typeLength
      FauxquetEncoder writeFieldEnd()
    }

    def writeName(): Unit = {
      FauxquetEncoder writeFieldBegin NAME_FIELD_DESC
      FauxquetEncoder writeString name
      FauxquetEncoder writeFieldEnd()
    }

    def writeFieldRepetitionType(): Unit = {
      FauxquetEncoder writeFieldBegin REPETITION_TYPE_FIELD_DESC
      FauxquetEncoder writeI32 fieldRepetitionType.id
      FauxquetEncoder writeFieldEnd()
    }
    def writeNumChildren(): Unit = {
      FauxquetEncoder writeFieldBegin NUM_CHILDREN_FIELD_DESC
      FauxquetEncoder writeI32 numChildren
      FauxquetEncoder writeFieldEnd()
    }

    def writeConvertedType(): Unit = {
      FauxquetEncoder writeFieldBegin CONVERTED_TYPE_FIELD_DESC
      FauxquetEncoder writeI32 convertedType.id
      FauxquetEncoder writeFieldEnd()
    }

    def writeScale(): Unit = {
      FauxquetEncoder writeFieldBegin SCALE_FIELD_DESC
      FauxquetEncoder writeI32 scale
      FauxquetEncoder writeFieldEnd()
    }

    def writePrecision(): Unit = {
      FauxquetEncoder writeFieldBegin PRECISION_FIELD_DESC
      FauxquetEncoder writeI32 precision
      FauxquetEncoder writeFieldEnd()
    }

    def writeFieldId(): Unit = {
      FauxquetEncoder writeFieldBegin FIELD_ID_FIELD_DESC
      FauxquetEncoder writeI32 fieldId
      FauxquetEncoder writeFieldEnd()
    }

    if (this.Type != null) {
      writeType()
    }

    if (typeLength != -1) {
      writeTypeLength()
    }

    if (this.fieldRepetitionType != null) {
      writeFieldRepetitionType()
    }

    if (this.name != null) {
      writeName()
    }

    if (this.numChildren != 0) {
      writeNumChildren()
    }

    if (this.convertedType != null) {
      writeConvertedType()
    }

    if (this.scale != -1) {
      writeScale()
    }

    if (this.precision != -1) {
      writePrecision()
    }

    if (this.fieldId != -1) {
      writeFieldId()
    }
  }

  override def validate(): Unit = {
    if (name == null) throw new Error("SchemaElement className was not found in file.")
  }

  override def className: String = "SchemaElement"
}