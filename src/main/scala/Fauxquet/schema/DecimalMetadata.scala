package main.scala.Fauxquet.schema

/**
  * Created by james on 1/28/17.
  */
class DecimalMetadata(val precision: Int, val scale: Int) {
  override def equals(o: scala.Any): Boolean = {
    if (!o.isInstanceOf[DecimalMetadata]) false
    else if (o == this) true
    else {
      val dm = o.asInstanceOf[DecimalMetadata]

      dm.precision == this.precision && dm.scale == this.scale
    }
  }
}