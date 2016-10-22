package main.scala.Fauxquet

/**
  * Created by james on 8/4/16.
  */
object Loader2 extends App {
  var sum = 0L

  for (i <- 0 until 30) {
    val t = time {
      val file = new FauxquetFile("./resources/lineitem.parquet")
      file init()
    }

    sum = sum + t
  }

  println(sum)

  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    //println("Elapsed time: " + (t1 - t0) + "ns")
    //result
    t1 - t0
  }
}