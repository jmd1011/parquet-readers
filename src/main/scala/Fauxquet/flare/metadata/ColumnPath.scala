package main.scala.Fauxquet.flare.metadata

/**
  * Created by james on 1/31/17.
  */
class ColumnPath(val path: Array[String]) extends Iterable[String] with Serializable {
  val paths = new Canonicalizer[ColumnPath]() {
    override def toCanonical(value: ColumnPath): ColumnPath = {
      val p: Array[String] = new Array[String](value.path.length)

      for (i <- value.path.indices) {
        p(i) = value.path(i).intern()
      }

      new ColumnPath(p)
    }
  }

  def fromDotString(str: String*): ColumnPath = {
    paths.canonicalize(new ColumnPath(str.toArray))
  }

  def toDotString: String = path.mkString(".")

  def pathSize: Int = path.length

  def toArray: Array[String] = path

  override def iterator: Iterator[String] = path.toList.iterator

  override def hashCode(): Int = path.hashCode()

  override def equals(o: scala.Any): Boolean = o.isInstanceOf[ColumnPath] && path.sameElements(o.asInstanceOf[ColumnPath].path)
}

object ColumnPathGetter {
  val paths = new Canonicalizer[ColumnPath]() {
    override def toCanonical(value: ColumnPath): ColumnPath = {
      val p: Array[String] = new Array[String](value.path.length)

      for (i <- value.path.indices) {
        p(i) = value.path(i).intern()
      }

      new ColumnPath(p)
    }
  }

  def get(path: Array[String]): ColumnPath = paths.canonicalize(new ColumnPath(path))
  def get(path: String*): ColumnPath = get(path.toArray)
}