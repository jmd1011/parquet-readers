package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream
import java.nio.ByteBuffer

/**
  * Created by james on 1/26/17.
  */
class UnsignedVarIntBytesInput(val value: Int) extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = {
    var v1 = value

    while ((v1 & 0xFFFFFF80) != 0L) {
      out.write((v1 & 0x7F) | 0x80)
      v1 >>>= 7
    }

    out.write(v1 & 0x7F)
  }

  override def size: Long = {
    val s = (38 - Integer.numberOfLeadingZeros(value)) / 7

    if (s == 0) 1
    else s
  }

  override def toByteBuffer: ByteBuffer = {
    var v1 = value
    val buf = ByteBuffer.allocate(this.size.asInstanceOf[Int])

    while ((v1 & 0xFFFFFF80) != 0L) {
      buf.putInt((v1 & 0x7F) | 0x80)
      v1 >>>= 7
    }

    buf.putInt(v1 & 0x7F)
  }
}
