package main.scala.Fauxquet.bytes.BytesInput

import java.io.OutputStream

import main.scala.Fauxquet.bytes.CapacityByteArrayOutputStream

/**
  * Created by james on 1/26/17.
  */
object CapacityBaosBytesInput extends BytesInput {
  var arrayOut: CapacityByteArrayOutputStream = _

  def apply(arrayOut: CapacityByteArrayOutputStream) = this.arrayOut = arrayOut

  override def writeAllTo(out: OutputStream): Unit = arrayOut.writeTo(out)
  override def size: Long = arrayOut.size
}
