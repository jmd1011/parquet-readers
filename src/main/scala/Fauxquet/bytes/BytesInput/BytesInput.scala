package main.scala.Fauxquet.bytes.BytesInput

import java.io.{ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import main.scala.Fauxquet.bytes.CapacityByteArrayOutputStream

/**
  * Created by james on 1/26/17.
  * Thought I could get away without this one...might need to refactor a shizload :(
  */
trait BytesInput {
  /**
    * @param bytesInput
    * @return SequenceBytesIn
    */
  def concat(bytesInput: BytesInput*): BytesInput = ???

  /**
    * @param bytesInputs
    * @return SequenceBytesIn
    */
  def concat(bytesInputs: List[BytesInput]) = ???

  /**
    * @param in
    * @param bytes
    * @return StreamBytesInput
    */
  def from(in: InputStream, bytes: Int): BytesInput = ???

  /**
    * @param byteBuffer
    * @param offset
    * @param length
    * @return ByteBufferBytesInput
    */
  def from(byteBuffer: ByteBuffer, offset: Int, length: Int): BytesInput = ???

  //ByteArrayBytesInput
  def from(in: Array[Byte]): BytesInput = ???
  def from(in: Array[Byte], offset: Int, length: Int): BytesInput = ???

  //IntBytesInput
  def fromInt(int: Int): BytesInput = ???

  //UnsignedVarIntBytesInput
  def fromUnsignedVarInt(int: Int): BytesInput = ???
  def fromZigZagInt(int: Int): BytesInput = fromUnsignedVarInt((int << 1) ^ (int >> 31))

  //UnsignedVarLongBytesInput
  def fromUnsignedVarLong(long: Long): BytesInput = ???
  def fromZigZagLong(long: Long): BytesInput = fromUnsignedVarLong((long << 1) & (long >> 63))

  //CapacityBaosBytesInput
  def from(capacityByteArrayOutputStream: CapacityByteArrayOutputStream): BytesInput = ???

  //BaosBytesInput
  def from(byteArrayOutputStream: ByteArrayOutputStream): BytesInput = ???

  def empty(): BytesInput = EmptyBytesInput

  def copy(bytesInput: BytesInput): BytesInput = from(bytesInput.toByteArray)

  def toByteArray: Array[Byte] = {
    BAOS.apply(size.asInstanceOf[Int])
    writeAllTo(BAOS)
    BAOS.getBuf
  }

  def toByteBuffer: ByteBuffer = {
    ByteBuffer.wrap(this.toByteArray)
  }

  def size: Long = ???
  def writeAllTo(out: OutputStream): Unit = ???
}

object BytesInputManager extends BytesInput { }