This project shows examples of how to read Apache Parquet files without using Apache Spark.

There are two different techniques in this codebase. #1 is currently (6/23/16) broken, but should be fixed soon.

1. [BROKEN] Loader.scala: Using SimpleRecordConverters, SimpleRecordMaterializers, etc., this technique moves all data in a Parquet file into SimpleRecord objects. Much of this code is based on http://www.programcreek.com/java-api-examples/index.php?source_dir=parquet-tools-master/src/main/java/parquet/tools/command/CatCommand.java, and can be thought of as a port to Scala with some improvements.

2. [WORKING] ParquetReader2.scala: This forgoes the use of Materializers, doing more of a data dump. This approach is much faster, as it keeps data in a columnar format. This code is again based on http://www.programcreek.com/java-api-examples/index.php?source_dir=parquet-tools-master/src/main/java/parquet/tools/command/DumpCommand.java, and can be thought of as a port to Scala with some improvements.
