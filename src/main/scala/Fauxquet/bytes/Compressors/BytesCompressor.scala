package main.scala.Fauxquet.bytes.Compressors

import main.scala.Fauxquet.bytes.BytesInput.BytesInput

/**
  * Created by james on 1/27/17.
  */
abstract class BytesCompressor {
  def compress(bytes: BytesInput): BytesInput
  def release(): Unit
}
