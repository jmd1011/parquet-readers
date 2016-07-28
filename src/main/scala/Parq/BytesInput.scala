package main.scala.Parq

import java.io.OutputStream

/**
  * Created by james on 7/19/16.
  */
abstract class BytesInput(size: Long) {
  def concat(inputs: BytesInput*) = new SequenceBytesIn(inputs toList)
  def writeAllTo(out: OutputStream): Unit = ???

  class SequenceBytesIn(inputs: List[BytesInput]) extends BytesInput(inputs.map(_.size).sum) {
    override def writeAllTo(out: OutputStream) = {
      def write(is: List[BytesInput]) = is match {
        case List() => Nil
        case x :: xs => x.writeAllTo(out)
      }
    }
  }

  class UnsignedVarIntBytesInput(value: Int) extends BytesInput(
    {
      val s = 5 - (Integer.numberOfLeadingZeros(value) + 3) / 7
      if (s == 0) 1L else s.asInstanceOf[Long]
    })
  {
    def size(): Long = {

    }

    override def writeAllTo(out: OutputStream): Unit = {
      var x = value & -128

      while (x.asInstanceOf[Long] != 0L) {
        out.write(x & 127 | 128)
        x >>>= 7
      }

      out.write(x & 127)
    }
  }
}