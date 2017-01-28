package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream

/**
  * Created by james on 1/27/17.
  */
class ConcatenatingByteArrayCollector extends BytesInput {
  var slabs = List[Array[Byte]]()
  var size_ = 0L

  def collect(bytesInput: BytesInput) = {
    val bytes = bytesInput.toByteArray
    slabs ::= bytes
    size_ += bytes.length
  }

  def reset() = {
    size_ = 0
    slabs = List[Array[Byte]]()
  }

  override def writeAllTo(out: OutputStream): Unit = slabs.foreach(out.write)

  override def size: Long = this.size_
}
