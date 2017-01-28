package main.scala.Fauxquet.column.ColumnWriters

import main.scala.Fauxquet.Encoders.RunLengthBitPackingHybridEncoder
import main.scala.Fauxquet.FauxquetObjs.Statistics
import main.scala.Fauxquet.ValuesReaders.rle.RunLengthBitPackingValuesWriter
import main.scala.Fauxquet.ValuesWriters.plain.PlainValuesWriter
import main.scala.Fauxquet.ValuesWriters.{DevNullValuesWriter, ValuesWriter}
import main.scala.Fauxquet.bytes.BytesInput.BytesInputManager
import main.scala.Fauxquet.bytes.{CapacityByteArrayOutputStream, HeapByteBufferAllocator}
import main.scala.Fauxquet.column.ColumnDescriptor

/**
  * Created by james on 1/26/17.
  * This only handles Parquet V1 due to copying all reading code from parquet-compat...can change to V2 if needed
  */
class ColumnWriterImpl(path: ColumnDescriptor, pageWriter: PageWriter) extends ColumnWriter {
  val MIN_SLAB_SIZE = 64
  val PAGE_SIZE = 1024 * 1024 //need to change this (maybe not -- this is what it is in parquet-compat)

  val repetitionLevelColumn: ValuesWriter = DevNullValuesWriter //for TPCH, will change later
  val definitionLevelColumn: ValuesWriter = new RunLengthBitPackingValuesWriter(
                                              new RunLengthBitPackingHybridEncoder(path.maxDef,
                                                new CapacityByteArrayOutputStream(MIN_SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator)
                                              )
                                            )
  val dataColumn:            ValuesWriter = new PlainValuesWriter(new CapacityByteArrayOutputStream(MIN_SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator))

  var valueCount = 0
  var valueCountForNextSizeCheck = 100 //MAGIC! it's pulled from ParquetProperties
  val statistics = new Statistics()
  //val writer = new ColumnChunkPageWriter(path)

  def accountForValueWritten(): Unit = {
    valueCount += 1

    if (valueCount > valueCountForNextSizeCheck) {
      val memSize = repetitionLevelColumn.bufferedSize() + definitionLevelColumn.bufferedSize() + dataColumn.bufferedSize()

      if (memSize > PAGE_SIZE) {
        valueCountForNextSizeCheck = valueCount / 2
        writePage()
      }
      else {
        valueCountForNextSizeCheck =
          (
            valueCount.asInstanceOf[Float] + valueCount.asInstanceOf[Float] * PAGE_SIZE.asInstanceOf[Float] / memSize.asInstanceOf[Float]
          ).asInstanceOf[Int] / 2 + 1
      }
    }
  }

  def writePage(): Unit = {
    pageWriter.writePage(
      BytesInputManager.concat(repetitionLevelColumn.toBytes, definitionLevelColumn.toBytes, dataColumn.toBytes),
      valueCount,
      statistics,
      repetitionLevelColumn.encoding,
      definitionLevelColumn.encoding,
      dataColumn.encoding
    )

    repetitionLevelColumn.reset()
    definitionLevelColumn.reset()
    dataColumn.reset()
    valueCount = 0
    //resetStatistics
  }

  override def write(value: Int, repetitionLevel: Int, definitionLevel: Int): Unit = {
    repetitionLevelColumn.writeInt(repetitionLevel)
    definitionLevelColumn.writeInt(definitionLevel)
    dataColumn.writeInt(value)
    //updateStatistics()
    accountForValueWritten()
  }

  override def write(value: Long, repetitionLevel: Int, definitionLevel: Int): Unit = {
    repetitionLevelColumn.writeInt(repetitionLevel)
    definitionLevelColumn.writeInt(definitionLevel)
    dataColumn.writeLong(value)
    //updateStatistics()
    accountForValueWritten()
  }

  override def write(value: Boolean, repetitionLevel: Int, definitionLevel: Int): Unit = {
    repetitionLevelColumn.writeInt(repetitionLevel)
    definitionLevelColumn.writeInt(definitionLevel)
    dataColumn.writeBoolean(value)
    //updateStatistics()
    accountForValueWritten()
  }

  override def write(value: Array[Byte], repetitionLevel: Int, definitionLevel: Int): Unit = {
    repetitionLevelColumn.writeInt(repetitionLevel)
    definitionLevelColumn.writeInt(definitionLevel)
    dataColumn.writeBytes(value)
    //updateStatistics()
    accountForValueWritten()
  }

  override def write(value: Float, repetitionLevel: Int, definitionLevel: Int): Unit = {
    repetitionLevelColumn.writeInt(repetitionLevel)
    definitionLevelColumn.writeInt(definitionLevel)
    dataColumn.writeFloat(value)
    //updateStatistics()
    accountForValueWritten()
  }

  override def write(value: Double, repetitionLevel: Int, definitionLevel: Int): Unit = {
    repetitionLevelColumn.writeInt(repetitionLevel)
    definitionLevelColumn.writeInt(definitionLevel)
    dataColumn.writeDouble(value)
    //updateStatistics()
    accountForValueWritten()
  }

  override def writeNull(repetitionLevel: Int, definitionLevel: Int): Unit = {
    repetitionLevelColumn.writeInt(repetitionLevel)
    definitionLevelColumn.writeInt(definitionLevel)
    //updateStatisticsNumNulls()
    accountForValueWritten()
  }
}
