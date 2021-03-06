package main.scala.Fauxquet.flare.metadata

import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.schema.{BaseType, GroupType, MessageType, OriginalType, PrimitiveType, PrimitiveTypeName, RepetitionManager}
import main.scala.Fauxquet.schema.OriginalType.OriginalType

/**
  * Created by james on 2/1/17.
  */
object FauxquetMetadataConverter {
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

  def fromFauxquetSchema(schema: List[SchemaElement]): MessageType = {
    new MessageType(null, schema.head.name, this.convertChildren(schema, schema.head.numChildren, 1))
  }

  def convertChildren(schema: List[SchemaElement], childrenCount: Int, offset: Int): List[BaseType] = {
    val result = new Array[BaseType](childrenCount)

    for (i <- result.indices) {
      val field = schema(i + offset)

      val repetition = RepetitionManager.getRepetitionByName(field.fieldRepetitionType.value.toUpperCase)
      val name = field.name

      result(i) = {
        if (field.Type != null) {
          val pField = FauxquetMetadataConverter.getType(field.Type)
          new PrimitiveType(repetition, pField, math.max(field.typeLength, 0), field.name, null, null, null) //TODO: This might screw things up?
        } else {
          new GroupType(repetition, name, null, this.convertChildren(schema, field.numChildren, i + 1), null)
        }
      }
    }

    result.toList
  }

  def fromFauxquetMetadata(metadata: FauxquetMetadata, schema: MessageType): FileMetadata = {
    val blks = metadata.blocks
    var rowGroups = List[RowGroup]()
    var numRows: Long = 0L

    for (block <- blks) {
      numRows += block.rowCount
      rowGroups :+= addRowGroup(block)
    }

    val fileMetadata = new FileMetadata(metadata.fileMetadata.schem) //TODO: May need to change how we're adding schema
    fileMetadata.schema = {
      var x = Vector[SchemaElement]()

      val root = fileMetadata.schem

      val rootSE = new SchemaElement(null)
      rootSE.numChildren = root.columns().size
      //rootSE.fieldRepetitionType = FieldRepetitionTypeManager.getFieldRepetitionTypeByName(root.repetition.name)
      rootSE.Type = null //Because "m" is GroupType
      rootSE.name = schema.name

      x :+= rootSE

      for (col <- fileMetadata.schem.columns()) {
        val se = new SchemaElement(rootSE)
        se.numChildren = 0 //for TPCH
        se.Type = getType(col.primitive)
        se.fieldRepetitionType = OPTIONAL //TODO: Need to get this dynamically (maybe)
        se.name = col.path(0)

        x :+= se
      }

      x
    }

    fileMetadata.numRows = numRows
    fileMetadata.rowGroups = rowGroups
    fileMetadata.createdBy = "Flare Team" //TODO
    fileMetadata.keyValueMetadata = List[KeyValue]()

    if (metadata.fileMetadata.keyValueMetadata != Nil) {
      for (kv <- metadata.fileMetadata.keyValueMetadata) {
        addKeyValue(fileMetadata, kv.key, kv.value)
      }
    }

    fileMetadata
  }

  def addKeyValue(fileMetadata: FileMetadata, key: String, value: String): Unit = {
    fileMetadata.keyValueMetadata :+= new KeyValue(key, value)
  }

  def addRowGroup(block: BlockMetadata): RowGroup = {
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

    val columns = block.columns
    var fauxquetColumns = List[ColumnChunk]()

    for (column <- columns) {
      val cc = new ColumnChunk()
      cc.fileOffset = column.firstDataPageOffset
      cc.filePath = block.path
      cc.metadata = new ColumnMetadata(getType(column.Type), column.encodings, column.path.toList, UNCOMPRESSED, column.valueCount, column.totalUncompressedSize, column.totalSize, column.firstDataPageOffset)
      cc.metadata.dictionaryPageOffset = column.dictionaryPageOffset

      fauxquetColumns :+= cc
    }

    val rowGroup = new RowGroup()
    rowGroup.columns = fauxquetColumns
    rowGroup.numRows = block.rowCount
    rowGroup.totalByteSize = block.totalBytesSize

    rowGroup
  }

  def fromFileMetadata(fileMetadata: FileMetadata, schema: List[SchemaElement]): FauxquetMetadata = {
    val schem = fromFauxquetSchema(schema)

    var blocks = List[BlockMetadata]()
    val rowGroups = fileMetadata.rowGroups

    if (rowGroups != null) {
      for (rg <- rowGroups) {
        val blockMetadata = new BlockMetadata
        blockMetadata.rowCount = rg.numRows
        blockMetadata.totalBytesSize = rg.totalByteSize

        val cols = rg.columns
        val filePath = cols.head.filePath

        for (cc <- cols) {
          if ((filePath == null && cc.filePath != null) || (filePath != null && cc.filePath == null)) throw new Error("Must be in the same file (for now)")

          val metadata = cc.metadata
          val path = getPath(metadata)
          val column = ColumnChunkMetadata.ColumnChunkMetadataManager.get(path, schem.getType(path.toArray).asPrimitiveType.primitive, metadata.dataPageOffset, metadata.dictionaryPageOffset, metadata.numValues, metadata.totalCompressedSize, metadata.totalUncompressedSize)
          blockMetadata.addColumn(column)
        }

        blockMetadata.path = filePath
        blocks :+= blockMetadata
      }
    }

    var keyValueMetadata = Map[String, String]()
    val keyValueMeta = fileMetadata.keyValueMetadata

    if (keyValueMeta != null) {
      keyValueMeta.foreach(kv => keyValueMetadata += (kv.key -> kv.value))
    }

    fileMetadata.schem = schem
    new FauxquetMetadata(fileMetadata, blocks)
  }

  def getPath(columnMetadata: ColumnMetadata): ColumnPath = {
    ColumnPathGetter.get(columnMetadata.pathInSchema.toArray)
  }
}