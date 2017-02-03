package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream
import java.nio.ByteBuffer

import main.scala.Fauxquet.Encoders.LittleEndianEncoder

/**
  * Created by james on 1/26/17.
  */
class IntBytesInput(val value: Int) extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = new LittleEndianEncoder(out).writeInt(this.value)
  override def size: Long = 4

  override def toByteBuffer = ByteBuffer.allocate(4).putInt(0, value)
}
