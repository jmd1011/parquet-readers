package main.scala.Fauxquet.bytes.BytesInput

import java.io.OutputStream

import main.scala.Fauxquet.bytes.CapacityByteArrayOutputStream

/**
  * Created by james on 1/26/17.
  */
class CapacityBaosBytesInput(val arrayOut: CapacityByteArrayOutputStream) extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = arrayOut.writeTo(out)
  override def size: Long = arrayOut.size
}
