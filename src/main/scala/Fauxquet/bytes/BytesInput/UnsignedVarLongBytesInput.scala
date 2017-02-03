package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream

/**
  * Created by james on 1/26/17.
  */
class UnsignedVarLongBytesInput(val value: Long) extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = {
    var v1 = value

    while ((v1 & 0xFFFFFFFFFFFFFF80L) != 0L) {
      out.write(((v1 & 0x7F) | 0x80).asInstanceOf[Int])
      v1 >>>= 7
    }

    out.write((v1 & 0x7F).asInstanceOf[Int])
  }

  override def size: Long = {
    val s = (70 - java.lang.Long.numberOfLeadingZeros(this.value)) / 7
    if (s == 0) 1
    else s
  }
}
