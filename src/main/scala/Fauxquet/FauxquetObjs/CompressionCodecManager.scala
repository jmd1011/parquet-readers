package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/9/16.
  */
object CompressionCodecManager {
  def getCodecById(i: Int): CompressionCodec = i match {
    case 0 => UNCOMPRESSED
    case 1 => SNAPPY
    case 2 => GZIP
    case 3 => LZO
    case _ => null
  }
}

trait CompressionCodec {
  val id: Int
  val value: String
}


object UNCOMPRESSED extends CompressionCodec {
  override val id: Int = 0
  override val value: String = "UNCOMPRESSED"
}

object SNAPPY extends CompressionCodec {
  override val id: Int = 1
  override val value: String = "SNAPPY"
}

object GZIP extends CompressionCodec {
  override val id: Int = 2
  override val value: String = "GZIP"
}

object LZO extends CompressionCodec {
  override val id: Int = 3
  override val value: String = "LZO"
}


