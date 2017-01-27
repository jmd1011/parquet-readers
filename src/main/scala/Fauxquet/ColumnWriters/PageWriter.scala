package main.scala.Fauxquet.ColumnWriters

import main.scala.Fauxquet.FauxquetObjs.{Encoding, Statistics}
import main.scala.Parq.DictionaryPage

/**
  * Created by james on 1/26/17.
  */
trait PageWriter {
  def writePage(bytes: Array[Byte], valueCount: Int, statistics: Statistics, rlEncoding: Encoding, dlEncoding: Encoding, valuesEncoding: Encoding): Unit

  def memSize: Long
  def allocatedSize: Long

  def writeDictionaryPage(dictionaryPage: DictionaryPage): Unit
}
