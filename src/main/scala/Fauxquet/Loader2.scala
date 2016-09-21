package main.scala.Fauxquet

/**
  * Created by james on 8/4/16.
  */
object Loader2 extends App {
  val t = time {
    val file = new FauxquetFile("./resources/customer.parquet")
    file init()
  }

  println(t)

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }
}