package main.scala.Fauxquet.ColumnWriters

import main.scala.Fauxquet.FauxquetObjs.{Encoding, Statistics}
import main.scala.Fauxquet.bytes.BytesInput.BytesInput
import main.scala.Fauxquet.page.DictionaryPage

/**
  * Created by james on 1/26/17.
  */
trait PageWriter {
  def writePage(bytes: BytesInput, valueCount: Int, statistics: Statistics, rlEncoding: Encoding, dlEncoding: Encoding, valuesEncoding: Encoding): Unit

  def memSize: Long
  def allocatedSize: Long

  def writeDictionaryPage(dictionaryPage: DictionaryPage): Unit
}
