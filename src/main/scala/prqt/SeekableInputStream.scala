package main.scala.prqt

import java.io.InputStream
import java.nio.ByteBuffer

/**
  * Created by jdecker on 7/1/16.
  */
abstract class SeekableInputStream extends InputStream {
  def position: Long

  def seek(newPos: Long): Unit
  def readFully(bytes: Array[Byte]): Unit
  def readFully(bytes: Array[Byte], start: Int, len: Int): Unit
  def readFully(buf: ByteBuffer): Unit
  def read(buf: ByteBuffer): Int
}
