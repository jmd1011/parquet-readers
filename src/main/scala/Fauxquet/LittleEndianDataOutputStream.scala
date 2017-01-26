package main.scala.Fauxquet

import java.io.OutputStream

/**
  * Created by james on 1/25/17.
  * Think maybe I can remove this class
  */
class LittleEndianDataOutputStream(out: OutputStream) extends OutputStream {
  override def write(i: Int): Unit = out.write(i)
  override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = out.write(bytes, offset, length)
  override def flush(): Unit = out.flush()

  def writeBoolean(b: Boolean): Unit = {
    out.write(if (b) 1 else 0)
  }


}