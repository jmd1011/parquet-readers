package main.scala.Fauxquet

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

/**
  * Created by james on 8/4/16.
  */
object Loader2 extends App {
  val arr = Files.readAllBytes(Paths.get("./resources/customer.parquet"))

  val l = arr.size

  val MAGIC = "PAR1".getBytes(Charset.forName("ASCII"))

  val footerLengthIndex = l - 4 - MAGIC.length

  val footerLength = {
    val x1 = arr(footerLengthIndex)
    val x2 = arr(footerLengthIndex + 1)
    val x3 = arr(footerLengthIndex + 2)
    val x4 = arr(footerLengthIndex + 3)

    if ((x1 | x2 | x3 | x4) < 0) throw new Error("Hit EOF early")

    (x4 << 24) + (x3 << 16) + (x2 << 8) + (x1 << 0)
  }

  val magic = new Array[Byte](MAGIC.length)

  for (i <- 0 until MAGIC.length) {
    magic(i) = arr(footerLengthIndex + 4 + i)
  }

  if (!magic.sameElements(MAGIC)) throw new Error("Not a Parquet file")

  val footerIndex = footerLengthIndex - footerLength
  val footer = arr.slice(footerIndex toInt, footerIndex.toInt + footerLength)
  val strFooter: String = new String(footer, "ASCII")

  println()
}