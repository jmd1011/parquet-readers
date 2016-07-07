package main.scala.prqt

/**
  * Created by jdecker on 6/24/16.
  */
class PFR extends java.io.Closeable {
  override def close(): Unit = ???

  val converter = new ParquetMetadataConverter1()
  def filter(skipRowGroups: Boolean): MetadataFilter = if (skipRowGroups) converter.SKIP_ROW_GROUPS else converter.NO_FILTER
}
