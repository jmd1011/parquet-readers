package main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata
import main.scala.Fauxquet.FauxquetObjs.{Encoding, INT32, TType}

/**
  * Created by james on 1/27/17.
  */
class IntColumnChunkMetadata(fdp: Long, dpo: Long, vc: Long, ts: Long, tuc: Long) extends ColumnChunkMetadata {
  val firstDataPage: Int = positiveLongToInt(fdp)
  val dictionaryPage = positiveLongToInt(dpo)
  val valCount: Int = positiveLongToInt(vc)
  val totSize: Int = positiveLongToInt(ts)
  val totUncompressedSize = positiveLongToInt(tuc)

  override def dictionaryPageOffset: Long = intToPositiveLong(dictionaryPage)
  override def firstDataPageOffset: Long = intToPositiveLong(firstDataPage)

  def positiveLongToInt(l: Long): Int = {
    if (l >= 0 && (l + Int.MinValue <= Int.MaxValue)) {
      (l + Int.MinValue).asInstanceOf[Int]
    }
    else throw new Error("Doesn't fit!")
  }

  def intToPositiveLong(i: Int): Long = i.asInstanceOf[Long] - Int.MinValue

  override def valueCount: Long = intToPositiveLong(valCount)

  override def totalSize: Long = intToPositiveLong(totSize)

  override def totalUncompressedSize: Long = intToPositiveLong(totUncompressedSize)

  override var Type: TType = INT32
  override var encodings: List[Encoding] = _
  override var path: Vector[String] = _
}
