package main.scala

import org.apache.parquet.schema.Type

/**
  * Created by jdecker on 6/21/16.
  */
class VectorizedParquetRecordReader extends SpecificParquetRecordReaderBase[Object] {
  private var batchIdx:   Int = 0
  private var numBatched: Int = 0

  override def initialize(path: String, columns: List[String]): Unit = {
    super.initialize(path, columns)
    initializeInternal()
  }

  def initializeInternal(): Unit = {
    val missingColumns = new Array[Boolean](requestedSchema.getFieldCount)

    for (i <- 0 to requestedSchema.getFieldCount) {
      val t: Type = requestedSchema.getFields.get(i)

      if (!t.isPrimitive || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new Exception("This is bad")
      }

      val colPath = requestedSchema.getPaths.get(i)
      if (fileSchema containsPath colPath) {
        val fd = fileSchema.getColumnDescription(colPath)

        if (!fd.equals(requestedSchema.getColumns.get(i))) {
          throw new Exception("also bad")
        }
      } else {
        if (requestedSchema.getColumns.get(i).getMaxDefinitionLevel == 0) {
          throw new Exception("whole mess of bad")
        }
      }

      missingColumns(i) = false
    }
  }
}
