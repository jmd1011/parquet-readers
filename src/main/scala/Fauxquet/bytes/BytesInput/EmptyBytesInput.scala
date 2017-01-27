package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream
import java.nio.ByteBuffer

/**
  * Created by james on 1/26/17.
  */
object EmptyBytesInput extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = { }
  override def size: Long = 0

  def toByteBuffer = ByteBuffer.allocate(0)
}
