package main.scala.Fauxquet

/**
  * Created by james on 8/4/16.
  */
object Loader2 extends App {
  var sum = 0L

  for (i <- 0 until 30) {
    var t0 = System.nanoTime()

    val file = new FauxquetFile("./resources/customer.parquet")
    file init()

    var t1 = System.nanoTime()

    sum += t1 - t0
  }

  println(s"Took $sum nanoseconds, ${sum / 1000000.0 / 30.0} milliseconds average")
}