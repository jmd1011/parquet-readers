package main.scala.Fauxquet.flare

import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.flare.api.WriteSupport
import main.scala.Fauxquet.schema.OriginalType.OriginalType
import main.scala.Fauxquet.schema.{BOOLEAN => _, DOUBLE => _, FIXED_LEN_BYTE_ARRAY => _, FLOAT => _, INT32 => _, INT64 => _, INT96 => _, _}

/**
  * Created by james on 8/5/16.
  */
class FauxquetFile() {
  type Schema = Vector[String]
  type Fields = Vector[String]

  case class Record(fields: Fields, schema: Schema) {
    def apply(key: String): String = fields(schema indexOf key)
    def apply(keys: Schema): Fields = keys.map(this apply _)
  }

  def Schema(schema: List[String]) = schema.toVector

  var data: Map[String, Vector[Any]] = Map[String, Vector[Any]]() //need to change this when dealing with Record
  var schema = Vector[SchemaElement]()
  var mtSchema: MessageType =_
  var fields: Fields = _

  def read(file: String) = {
    val fauxquetReader = new FauxquetReader(file)
    data = fauxquetReader.read()
    schema = fauxquetReader.fileMetaData.schema
    mtSchema = fromParquetSchema(schema.toList)
  }

  def fromParquetSchema(schema: List[SchemaElement]): MessageType = {
    var fields: List[BaseType] = List[BaseType]()

    for (field <- schema) {
      if (field.Type != null) {
        val pField = getType(field.Type)
        fields ::= new PrimitiveType(RepetitionManager.getRepetitionByName(field.fieldRepetitionType.value.toUpperCase), pField, field.typeLength, field.name, getOriginalType(field.convertedType), new DecimalMetadata(field.precision, field.scale), new ID(field.fieldId))
      } else {
                                                                                                                                       //TPCH won't have structured data
        fields ::= new GroupType(RepetitionManager.getRepetitionByName(field.fieldRepetitionType.value.toUpperCase), field.name, null, List[BaseType](), null)
      }
    }

    new MessageType(schema.head.name, fields)
  }

  def getOriginalType(convertedType: ConvertedType): OriginalType = {
    convertedType match {
      case UTF8 => OriginalType.UTF8
      case MAP => OriginalType.MAP
      case MAP_KEY_VALUE => OriginalType.MAP_KEY_VALUE
      case LIST => OriginalType.LIST
      case ENUM => OriginalType.ENUM
      case DECIMAL => OriginalType.DECIMAL
      case DATE => OriginalType.DATE
      case TIME_MILLIS => OriginalType.TIME_MILLIS
      //case TIME_MICROS => OriginalType.TIME_MICROS
      case TIMESTAMP_MILLIS => OriginalType.TIMESTAMP_MILLIS
      //case TIMESTAMP_MICROS => OriginalType.TIMESTAMP_MICROS
      case INTERVAL => OriginalType.INTERVAL
      case INT_8 => OriginalType.INT_8
      case INT_16 => OriginalType.INT_16
      case INT_32 => OriginalType.INT_32
      case INT_64 => OriginalType.INT_64
      case UINT_8 => OriginalType.UINT_8
      case UINT_16 => OriginalType.UINT_16
      case UINT_32 => OriginalType.UINT_32
      case UINT_64 => OriginalType.UINT_64
      case JSON => OriginalType.JSON
      case BSON => OriginalType.BSON
      case _ => throw new Error("Unknown converted type")
    }
  }

  def getType(tType: TType): PrimitiveTypeName = tType match {
    case INT32 => main.scala.Fauxquet.schema.INT32
    case INT64 => main.scala.Fauxquet.schema.INT64
    case BOOLEAN => main.scala.Fauxquet.schema.BOOLEAN
    case BYTE_ARRAY => main.scala.Fauxquet.schema.BINARY
    case FLOAT => main.scala.Fauxquet.schema.FLOAT
    case DOUBLE => main.scala.Fauxquet.schema.DOUBLE
    case INT96 => main.scala.Fauxquet.schema.INT96
    case FIXED_LEN_BYTE_ARRAY => main.scala.Fauxquet.schema.FIXED_LEN_BYTE_ARRAY
  }

  def getType(primitiveTypeName: PrimitiveTypeName): TType = primitiveTypeName match {
    case main.scala.Fauxquet.schema.INT32 => INT32
    case main.scala.Fauxquet.schema.INT64 => INT64
    case main.scala.Fauxquet.schema.BOOLEAN => BOOLEAN
    case main.scala.Fauxquet.schema.BINARY => BYTE_ARRAY
    case main.scala.Fauxquet.schema.FLOAT => FLOAT
    case main.scala.Fauxquet.schema.DOUBLE => DOUBLE
    case main.scala.Fauxquet.schema.INT96 => INT96
    case main.scala.Fauxquet.schema.FIXED_LEN_BYTE_ARRAY => FIXED_LEN_BYTE_ARRAY
    case _ => throw new Error("Unknown type encountered")
  }

  def write(file: String, schema: MessageType) = {
    val fauxquetWriter = new FauxquetWriter(file, new WriteSupport(schema))

    var test = List[String]()

    for (f <- data) {
      test ::= f.toString()
    }

    fauxquetWriter.write(test)
  }
}
