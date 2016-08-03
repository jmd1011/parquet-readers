package main.scala.Parq

import java.io.{Closeable, DataInputStream, InputStream}

/**
  * Created by James on 8/2/2016.
  */
class FSDataInputStream(inputStream: InputStream) extends DataInputStream(inputStream) with Seekable with PositionedReadable with Closeable {
  override def seek(pos: Long): Unit = in.asInstanceOf[Seekable].seek(pos)
  override def getPos: Long = in.asInstanceOf[Seekable].getPos
  override def seekToNewSource(pos: Long): Boolean = in.asInstanceOf[Seekable].seekToNewSource(pos)

  override def read(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Int = in.asInstanceOf[PositionedReadable].read(pos, buffer, offset, length)
  override def readFully(pos: Long, buffer: Array[Byte]): Unit = in.asInstanceOf[PositionedReadable].readFully(pos, buffer, 0, buffer length)
  override def readFully(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = in.asInstanceOf[PositionedReadable].readFully(pos, buffer, offset, length)

  if (!inputStream.isInstanceOf[Seekable] || !in.isInstanceOf[PositionedReadable]) throw new Error("inputStream must be Seekable and PositionedReadable")
}

trait Seekable {
  def seek(pos: Long)
  def getPos: Long
  def seekToNewSource(pos: Long): Boolean
}

trait PositionedReadable {
  def read(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Int
  def readFully(pos: Long, buffer: Array[Byte])
  def readFully(pos: Long, buffer: Array[Byte], offset: Int, length: Int)
}