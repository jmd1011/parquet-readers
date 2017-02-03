package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet.FauxquetObjs.statistics.Statistics
import main.scala.Fauxquet._

/**
  * Created by james on 8/30/16.
  */
class DataPageHeader(var numValues: Int = -1, var encoding: Encoding = null, var definitionLevelEncoding: Encoding = null, var repetitionLevelEncoding: Encoding = null, var statistics: Statistics = null) extends Fauxquetable {
  //var numValues = -1

  //var encoding: Encoding = _
  //var definitionLevelEncoding: Encoding = _
  //var repetitionLevelEncoding: Encoding = _

  //var statistics: Statistics = _

  private val NUM_VALUES_FIELD_DESC = TField("num_values", 8, 1)
  private val ENCODING_FIELD_DESC = TField("encoding", 8, 2)
  private val DEFINITION_LEVEL_ENCODING_FIELD_DESC = TField("definition_level_encoding", 8, 3)
  private val REPETITION_LEVEL_ENCODING_FIELD_DESC = TField("repetition_level_encoding", 8, 4)
  private val STATISTICS_FIELD_DESC = TField("statistics", 12, 5)

  override def className: String = "DataPageHeader"

  override def validate(): Unit = {
    if (numValues == -1) throw new Error("DataPageHeader numValues was not found in file.")
    if (encoding == null) throw new Error("DataPageHeader encoding was not found in file.")
    if (definitionLevelEncoding == null) throw new Error("DataPageHeader definitionLevelEncoding was not found in file.")
    if (repetitionLevelEncoding == null) throw new Error("DataPageHeader repetitionLevelEncoding was not found in file.")
  }

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => numValues = FauxquetDecoder readI32 arr
      case 2 => encoding = EncodingManager getEncodingById(FauxquetDecoder readI32 arr)
      case 3 => definitionLevelEncoding = EncodingManager getEncodingById(FauxquetDecoder readI32 arr)
      case 4 => repetitionLevelEncoding = EncodingManager getEncodingById(FauxquetDecoder readI32 arr)
      case _ => FauxquetDecoder skip(arr, 8)
    }
    case TField(_, 12, 5) =>
      statistics = new Statistics()
      statistics.read(arr)
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def doWrite(): Unit = {
    def writeNumValues(): Unit = {
      FauxquetEncoder writeFieldBegin ENCODING_FIELD_DESC
      FauxquetEncoder writeI32 numValues
      FauxquetEncoder writeFieldEnd()
    }

    def writeEncoding(): Unit = {
      FauxquetEncoder writeFieldBegin ENCODING_FIELD_DESC
      FauxquetEncoder writeI32 encoding.id
      FauxquetEncoder writeFieldEnd()
    }

    def writeDefinitionLevelEncoding(): Unit = {
      FauxquetEncoder writeFieldBegin DEFINITION_LEVEL_ENCODING_FIELD_DESC
      FauxquetEncoder writeI32 definitionLevelEncoding.id
      FauxquetEncoder writeFieldEnd()
    }

    def writeRepetitionLevelEncoding(): Unit = {
      FauxquetEncoder writeFieldBegin REPETITION_LEVEL_ENCODING_FIELD_DESC
      FauxquetEncoder writeI32 repetitionLevelEncoding.id
      FauxquetEncoder writeFieldEnd()
    }

    def writeStatistics(): Unit = {
      FauxquetEncoder writeFieldBegin STATISTICS_FIELD_DESC
      this.statistics.write()
      FauxquetEncoder writeFieldEnd()
    }

    writeNumValues()

    if (this.encoding != null) {
      writeEncoding()
    }

    if (this.definitionLevelEncoding != null) {
      writeDefinitionLevelEncoding()
    }

    if (this.repetitionLevelEncoding != null) {
      writeRepetitionLevelEncoding()
    }

    if (this.statistics != null) {
      writeStatistics()
    }
  }
}