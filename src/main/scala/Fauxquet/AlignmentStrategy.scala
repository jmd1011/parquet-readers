package main.scala.Fauxquet

/**
  * Created by james on 1/10/17.
  */
trait AlignmentStrategy {
  def alignForRowGroup(fauxquetOutputStream: FauxquetOutputStream): Unit
  def nextRowGroupSize(fauxquetOutputStream: FauxquetOutputStream): Long

  var initialized = false
}

object NoAlignment extends AlignmentStrategy {
  private var rowGroupSize: Long = 128 * 1024 *10241

  def init(rowGroupSize: Long) = {
    this.rowGroupSize = rowGroupSize
    initialized = true
  }

  override def alignForRowGroup(fauxquetOutputStream: FauxquetOutputStream): Unit = { }

  override def nextRowGroupSize(fauxquetOutputStream: FauxquetOutputStream): Long = {
    //if (!initialized) throw new Error("Need to initialize NoAlignment!")

    this.rowGroupSize
  }
}

object PaddingAlignment extends AlignmentStrategy {
  var dfsBlockSize: Long = 0L
  var rowGroupSize: Long = 0L
  var maxPaddingSize: Int = 0

  val zeros = new Array[Byte](4096)

  def init(dfsBlockSize: Long, rowGroupSize: Long, maxPaddingSize: Int) = {
    this.dfsBlockSize = dfsBlockSize
    this.rowGroupSize = rowGroupSize
    this.maxPaddingSize = maxPaddingSize

    initialized = true
  }

  def isPaddingNeeded(remaining: Long) = remaining <= maxPaddingSize

  override def alignForRowGroup(fauxquetOutputStream: FauxquetOutputStream): Unit = {
    if (!initialized) throw new Error("Need to initialize PaddingAlignment")

    val remaining: Long = dfsBlockSize - (fauxquetOutputStream.pos % dfsBlockSize)

    if (isPaddingNeeded(remaining)) {
      var i = remaining

      while (i > 0) {
        fauxquetOutputStream.write(zeros, 0, math.min(zeros.length.asInstanceOf[Long], remaining).asInstanceOf[Int])

        i -= zeros.length
      }
    }
  }

  override def nextRowGroupSize(fauxquetOutputStream: FauxquetOutputStream): Long = {
    if (!initialized) throw new Error("Need to initialize PaddingAlignment")

    if (maxPaddingSize <= 0) {
      return rowGroupSize
    }

    val remaining = dfsBlockSize - (fauxquetOutputStream.pos % dfsBlockSize)

    if (isPaddingNeeded(remaining)) {
      return rowGroupSize
    }

    math.min(remaining, rowGroupSize)
  }
}
