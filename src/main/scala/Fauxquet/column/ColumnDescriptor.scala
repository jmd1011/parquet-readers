package main.scala.Fauxquet.column

import java.util

import main.scala.Fauxquet.schema.PrimitiveTypeName

/**
  * Created by james on 1/26/17.
  */
class ColumnDescriptor(val path: Array[String], val primitive: PrimitiveTypeName, val typeLength: Int = 0, val maxRep: Int, val maxDef: Int) extends Comparable[ColumnDescriptor] {
  override def hashCode(): Int = util.Arrays.hashCode(path.asInstanceOf[Array[AnyRef]])

  override def equals(o: scala.Any): Boolean =
    if (o == null) false
    else if (!o.isInstanceOf[ColumnDescriptor]) false
    else util.Arrays.equals(path.asInstanceOf[Array[AnyRef]], o.asInstanceOf[ColumnDescriptor].path.asInstanceOf[Array[AnyRef]])

  override def compareTo(t: ColumnDescriptor): Int = {
    val length = math.min(path.length, t.path.length)

    for (i <- 0 until length) {
      val compareTo = path(i).compareTo(t.path(i))

      if (compareTo != 0) return compareTo
    }

    path.length - t.path.length
  }
}
