package main.scala.Fauxquet.ColumnWriters

import main.scala.Fauxquet.Encoders.RunLengthBitPackingHybridEncoder
import main.scala.Fauxquet.FauxquetObjs.Statistics
import main.scala.Fauxquet.ValuesReaders.rle.RunLengthBitPackingValuesWriter
import main.scala.Fauxquet.ValuesWriters.plain.PlainValuesWriter
import main.scala.Fauxquet.ValuesWriters.{DevNullValuesWriter, ValuesWriter}
import main.scala.Fauxquet.bytes.{CapacityByteArrayOutputStream, HeapByteBufferAllocator}

/**
  * Created by james on 1/26/17.
  * This is called ColumnWriterV1 in Parquet (I think?)...there's also a V2 in there. Based on parquet-compatibility, this is what I should be using
  */
class ColumnWriterImpl(maxRep: Int, maxDef: Int) {
  val MIN_SLAB_SIZE = 64
  val PAGE_SIZE = 1024 * 1024 //need to change this (maybe not -- this is what it is in parquet-compat)

  val repetitionLevelColumn: ValuesWriter = DevNullValuesWriter //for TPCH, will change later
  val definitionLevelColumn: ValuesWriter = new RunLengthBitPackingValuesWriter(
                                              new RunLengthBitPackingHybridEncoder(maxDef,
                                                new CapacityByteArrayOutputStream(MIN_SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator)
                                              )
                                            )
  val dataColumn:            ValuesWriter = new PlainValuesWriter(new CapacityByteArrayOutputStream(MIN_SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator))

  var valueCount = 0
  var valueCountForNextSizeCheck = 100 //MAGIC! it's pulled from ParquetProperties
  val statistics = new Statistics()

  def accountForValueWritten(): Unit = {
    valueCount += 1

    if (valueCount > valueCountForNextSizeCheck) {
      val memSize = repetitionLevelColumn.bufferedSize() + definitionLevelColumn.bufferedSize() + dataColumn.bufferedSize()

      if (memSize > PAGE_SIZE) {
        valueCountForNextSizeCheck = valueCount / 2
        writePage()
      }
    }
  }

  def writePage(): Unit = {

  }
}
