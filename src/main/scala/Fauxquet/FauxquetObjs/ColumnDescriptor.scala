package main.scala.Fauxquet.FauxquetObjs

import java.util

/**
  * Created by james on 1/26/17.
  */
class ColumnDescriptor(path: Array[String], tType: TType, typeLength: Int = 0, maxRep: Int, maxDef: Int) extends Comparable[ColumnDescriptor] {
  override def hashCode(): Int = util.Arrays.hashCode(path.asInstanceOf[Array[AnyRef]])

  override def equals(o: scala.Any): Boolean =
    if (o == this) true
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
