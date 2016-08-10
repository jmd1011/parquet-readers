package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
object CompressionCodecManager {
  def getCodecById(i: Int): CompressionCodec = i match {
    case 0 => CompressionCodec(0, "UNCOMPRESSED")
    case 1 => CompressionCodec(1, "SNAPPY")
    case 2 => CompressionCodec(2, "GZIP")
    case 3 => CompressionCodec(3, "LZO")
    case _ => null
  }
}
