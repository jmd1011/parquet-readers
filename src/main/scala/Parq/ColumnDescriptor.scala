package main.scala.Parq

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
  * Created by James on 8/1/2016.
  */
class ColumnDescriptor(val path: Array[String], val _type: PrimitiveTypeName, val typeLength: Int, val maxRep: Int, val maxDef: Int) extends Comparable[ColumnDescriptor] {
  def this(path: Array[String], _type: PrimitiveTypeName, maxRep: Int, maxDef: Int) = this(path, _type, 0, maxRep, maxDef)

//  def equals(other: Object): Boolean = {
//    other == this || !other.isInstanceOf[ColumnDescriptor] || other.asInstanceOf[ColumnDescriptor].path.sameElements(this.path)
//  }

  override def compareTo(o: ColumnDescriptor): Int = {
    for (i <- this.path.indices) {
      val compareTo = this.path(i).compareTo(o.path(i))

      if (compareTo != 0) {
        return compareTo
      }
    }

    0
  }
}
