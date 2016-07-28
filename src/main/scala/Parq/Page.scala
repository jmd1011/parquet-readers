package main.scala.Parq

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.Encoding

/**
  * Created by james on 7/19/16.
  */
abstract class Page {
  var compressedSize: Int
  var uncompressedSize: Int

  def this(compressedSize: Int, uncompressedSize: Int) = {
    this()
    this.compressedSize = compressedSize
    this.uncompressedSize = uncompressedSize
  }
}

class DictionaryPage extends Page {
  var compressedSize: Int = _
  var uncompressedSize: Int = _
  var dictionarySize: Int = _
  var encoding: Encoding = _
  var bytes: BytesInput = _

  def this(bytes: BytesInput, uncompressedSize: Int, dictionarySize: Int, encoding: Encoding) {
    this()
    this.bytes = bytes
    this.compressedSize = bytes.size().asInstanceOf[Int]
    this.uncompressedSize = uncompressedSize
    this.dictionarySize = dictionarySize

    if (encoding == null) throw new Error("Encoding can not be null!")

    this.encoding = encoding
  }

  def this(bytes: BytesInput, dictionarySize: Int, encoding: Encoding) {
    this(bytes, bytes.size().asInstanceOf[Int], dictionarySize, encoding)
  }

  def copy: DictionaryPage = new DictionaryPage(BytesInput.copy(this.bytes), this.uncompressedSize, this.dictionarySize, this.encoding)
}