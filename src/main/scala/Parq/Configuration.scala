package main.scala.Parq

import java.io.{DataInput, DataOutput}
import java.util.Map.Entry

/**
  * Created by James on 7/11/2016.
  */
class Configuration extends Iterable[Entry[String, String]] with Writable {
  override def write(out: DataOutput): Unit = {

  }

  override def readFields(in: DataInput): Unit = {

  }

  override def iterator: Iterator[Entry[String, String]] = ???
}

trait Writable {
  def write(out: DataOutput)
  def readFields(in: DataInput)
}