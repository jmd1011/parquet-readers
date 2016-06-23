This project shows examples of how to read Apache Parquet files without using Apache Spark.

There are currently two different techniques in this codebase.

1. Loader.scala: Using SimpleRecordConverters, SimpleRecordMaterializers, etc., this technique moves all data in a Parquet file into SimpleRecord objects. Much of this code is based on http://www.programcreek.com/java-api-examples/index.php?source_dir=parquet-tools-master/src/main/java/parquet/tools/read/SimpleRecordConverter.java, and can be thought of as a port to Scala with some improvements.

2. ParquetReader2.scala: This forgoes the use of Materializers, doing more of a data dump. This approach is much faster, but does not currently create any records.
