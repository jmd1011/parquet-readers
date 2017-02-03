package main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata
import main.scala.Fauxquet.FauxquetObjs.{BIT_PACKED, Encoding, PLAIN, RLE}
import main.scala.Fauxquet.flare.metadata.ColumnPath
import main.scala.Fauxquet.schema.PrimitiveTypeName

/**
  * Created by james on 1/27/17.
  */
class IntColumnChunkMetadata(p: ColumnPath, primName: PrimitiveTypeName, fdp: Long, dpo: Long, vc: Long, ts: Long, tuc: Long) extends ColumnChunkMetadata {
  val firstDataPage: Int = positiveLongToInt(fdp)
  val dictionaryPage = positiveLongToInt(dpo)
  val valCount: Int = positiveLongToInt(vc)
  val totSize: Int = positiveLongToInt(ts)
  val totUncompressedSize = positiveLongToInt(tuc)

  override def dictionaryPageOffset: Long = intToPositiveLong(dictionaryPage)
  override def firstDataPageOffset: Long = intToPositiveLong(firstDataPage)

  def positiveLongToInt(l: Long): Int = {
    if (l >= 0 && (l + Int.MinValue <= Int.MaxValue)) {
      (l + 0).asInstanceOf[Int] //Parquet-MR claims this should be l + Int.MinValue, parquet-compat disagrees (and I agree with parquet-compat)
    }
    else throw new Error("Doesn't fit!")
  }

  def intToPositiveLong(i: Int): Long = i.asInstanceOf[Long] - 0//Int.MinValue

  override def valueCount: Long = intToPositiveLong(valCount)

  override def totalSize: Long = intToPositiveLong(totSize)

  override def totalUncompressedSize: Long = intToPositiveLong(totUncompressedSize)

  //override var Type: PrimitiveTypeName = INT32
  override var encodings: List[Encoding] = List[Encoding](BIT_PACKED, RLE, PLAIN) //TODO: Just for TPCH
  override val path: ColumnPath = p
  override val Type: PrimitiveTypeName = primName
}
