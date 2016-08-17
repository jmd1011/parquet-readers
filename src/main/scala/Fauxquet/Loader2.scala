package main.scala.Fauxquet

/**
  * Created by james on 8/4/16.
  */
object Loader2 extends App {
  val file = new FauxquetFile("./resources/customer.parquet")
  file init()

  println()
}