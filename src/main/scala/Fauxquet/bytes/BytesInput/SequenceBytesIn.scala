package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream

/**
  * Created by james on 1/26/17.
  */
object SequenceBytesIn extends BytesInput {
  var inputs: List[BytesInput] = _
  var s: Long = 0

  def apply(inputs: List[BytesInput]) = {
    this.inputs = inputs
    s = (0L /: inputs) (_ + _.size)
  }

  override def size: Long = this.s

  override def writeAllTo(out: OutputStream): Unit = inputs.foreach(_.writeAllTo(out))
}
