package main.scala.Fauxquet.bytes

import java.nio.ByteBuffer

/**
  * Created by james on 1/25/17.
  */
trait ByteBufferAllocator {
  def allocate(size: Int): ByteBuffer
  def release(bb: ByteBuffer): Unit
  var isDirect: Boolean
}
