package main.scala.Fauxquet

import java.io.{DataOutputStream, FilterOutputStream, OutputStream}

import main.scala.Fauxquet.FauxquetObjs.Statistics

/**
  * Created by james on 1/10/17.
  */
class FauxquetOutputStream(fout: OutputStream) extends DataOutputStream(fout) {


  def pos = {
    this.fout.asInstanceOf[PositionCache].pos
  }

  class PositionCache(val fout: OutputStream, val stats: Statistics, var pos: Long) extends FilterOutputStream(fout) {
    override def write(b: Int) = {
      this.out.write(b)
      this.pos += 1

      if (this.stats != null) { //TODO: fix stats

      }
    }

    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      this.out.write(bytes, offset, length)
      this.pos += length.asInstanceOf[Long]

      if (this.stats != null) { //TODO: fix stats

      }
    }

    override def close(): Unit = this.out.close()
  }
}