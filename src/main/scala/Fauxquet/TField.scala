package main.scala.Fauxquet

/**
  * Created by james on 8/5/16.
  */

case class TField(name: String, Type: Byte, id: Short) {
  def this() = this("", 0, 0)

  def apply(name: String, Type: Byte, id: Short) = new TField(name, Type, id)

  def equals(o: TField): Boolean = o.name == this.name && o.Type == this.Type && o.id == this.id
  override def equals(o: scala.Any): Boolean = if (!o.isInstanceOf[TField]) false else this.equals(o.asInstanceOf[TField])
}

object TSTOP extends TField("", 0, 0)