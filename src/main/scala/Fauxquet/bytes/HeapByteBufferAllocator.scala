package main.scala.Fauxquet.bytes
import java.nio.ByteBuffer

/**
  * Created by james on 1/26/17.
  */
object HeapByteBufferAllocator extends ByteBufferAllocator {
  override def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size)

  override def release(bb: ByteBuffer): Unit = {}

  override val isDirect: Boolean = false
}
