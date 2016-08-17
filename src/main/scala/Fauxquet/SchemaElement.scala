package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
class SchemaElement extends Fauxquetable {
  var name: String = _
  var numChildren: Int = -1
  var scale: Int = -1
  var precision: Int = -1
  var fieldId: Int = -1

  var Type: TType = _
  var typeLength: Int = -1

  var fieldRepetitionType: FieldRepetitionType = _
  var convertedType: ConvertedType = _

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => Type = TTypeManager getType(FauxquetDecoder readI32 arr)
      case 2 => typeLength = FauxquetDecoder readI32 arr
      case 3 => fieldRepetitionType = FieldRepetitionTypeManager getFieldRepetitionTypeById(FauxquetDecoder readI32 arr)
      case 5 => numChildren = FauxquetDecoder readI32 arr
      case 6 => convertedType = ConvertedTypeManager getConvertedTypeById(FauxquetDecoder readI32 arr)
      case 7 => scale = FauxquetDecoder readI32 arr
      case 8 => precision = FauxquetDecoder readI32 arr
      case 9 => fieldId = FauxquetDecoder readI32 arr
    }
    case TField(_, 11, 4) => name = FauxquetDecoder readString arr
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  //TODO
  override def write(): Unit = ???

  override def validate(): Unit = {
    if (className == null) throw new Error("SchemaElement className was not found in file.")
  }

  override def className: String = "SchemaElement"
}