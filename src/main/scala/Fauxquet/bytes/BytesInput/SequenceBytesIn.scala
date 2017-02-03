package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream

/**
  * Created by james on 1/26/17.
  */
class SequenceBytesIn(val inputs: List[BytesInput]) extends BytesInput {
  val s: Long = (0L /: this.inputs) (_ + _.size)

  override def size: Long = this.s

  override def writeAllTo(out: OutputStream): Unit = {
    inputs.foreach(_.writeAllTo(out))
  }
}