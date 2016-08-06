package main.scala.Fauxquet

import java.io.OutputStream

/**
  * Created by james on 8/5/16.
  * Recommend listening to Frank Sinatra's "Unfauxquetable" while extending
  */
trait Fauxquetable {
  def read(decoder: Fauxquetable)
  def write(out: OutputStream) //writes `this` to OutputStream TODO: Make this a FauxquetOutputStream
}
