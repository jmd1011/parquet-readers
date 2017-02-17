package main.scala.Fauxquet.flare

import java.nio.file.{Files, Paths}

/**
  * Created by james on 2/13/17.
  */
class MetadataReader(path: String) {
  val MAGIC = Array[Byte](80, 65, 82, 49)

  def read(): ParquetMetadata = {
    def doRead(i: Int, id: Int)(version: Int, schema: List[SchemaElement], numRows: Long, rowGroups: List[RowGroup], createdBy: String): ParquetMetadata = {
      def readListBegin(i: Int): (Int, TList) = {
        val sizeAndType = array(i)

        val size = sizeAndType >> 4 & 15

        if (size != 15) (i + 1, TList(getType(sizeAndType), size))
        else {
          val (i1, size1) = readVarint32(i + 1)
          (i1, TList(getType(sizeAndType), size1))
        }
      }

      def readFieldBegin(i: Int, id: Int): (Int, TField) = {
        val t = array(i)

        if (t == 0) (i + 1, TSTOP)
        else {
          val modifier = ((t & 240) >> 4).asInstanceOf[Short]

          val (i1, fid) = {
            if (modifier == 0) readI16(i + 1)
            else (i + 1, (id + modifier).asInstanceOf[Short])
          }

          val field = TField(getType((t & 15).asInstanceOf[Byte]), fid)

          //if (isBoolean(t)) this.boolValue = (t & 15).asInstanceOf[Byte] == 1
          //          this.id = fid

          (i1, field)
        }
      }

      def readI16(i: Int): (Int, Short) = {
        val (i1, r) = readVarint32(i)
        val ret = zigzagToInt(r)
        (i1, ret.asInstanceOf[Short])
      }

      def readInt(i: Int): (Int, Int) = {
        val ch4 = array(i) & 255
        val ch3 = array(i + 1) & 255
        val ch2 = array(i + 2) & 255
        val ch1 = array(i + 3) & 255

        val ret = (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0)
        (i + 4, ret)
      }

      def readI32(i: Int): (Int, Int) = {
        val (i1, r) = readVarint32(i)
        val ret = zigzagToInt(r)
        (i1, ret)
      }

      def readI64(i: Int): (Int, Long) = {
        val (i1, r) = readVarint64(i)
        val ret = zigzagToLong(r)
        (i1, ret)
      }

      def readVarint32(i: Int): (Int, Int) = {
        def readVarint32(i: Int, shift: Int, res: Int): (Int, Int) = {
          val b1 = array(i)
          val res1 = res | ((b1 & 127) << shift)

          if ((b1 & 128) != 128) (i + 1, res1)
          else readVarint32(i + 1, shift + 7, res1)
        }

        readVarint32(i, 0, 0)
      }

      def readVarint64(i: Int): (Int, Long) = {
        def readVarint64(i: Int, shift: Int, res: Long): (Int, Long) = {
          val b1 = array(i)
          val res1 = res | ((b1 & 127).asInstanceOf[Long] << shift)

          if ((b1 & 128) != 128) (i + 1, res1)
          else readVarint64(i + 1, shift + 7, res1)
        }

        readVarint64(i, 0, 0)
      }

      def zigzagToInt(n: Int):   Int  = n >>> 1 ^ -(n & 1)
      def zigzagToLong(n: Long): Long = n >>> 1 ^ -(n & 1L)

      def readString(i: Int): (Int, String) = {
        val (i2, length) = readVarint32(i)

        if (length == 0) (i2, "")
        else {
          val (i3, bytes) = readBinary(i2, length)
          (i3, new String(bytes, "UTF-8"))
        }
      }

      def readBinary(i: Int, length: Int): (Int, Array[Byte]) = {
        if (length == 0) (i, new Array[Byte](0))
        else {
          val buf = new Array[Byte](length)

          for (j <- 0 until length) {
            buf(j) = array(i + j)
          }

          (i + length, buf)
        }
      }

      def skip(i: Int, tpe: Byte) : Int = {
        def skip(i: Int, id: Int, tpe: Byte): Int = tpe match {
          case 2 => i + 1 //maybe?
          case 3 => i + 1
          case 4  => i + 8
          case 8 => val (i1, _) = readI32(i)
            i1
          case 10 => val (i1, _) = readI64(i)
            i1
          case 11 =>
            val (i1, _) = readString(i)
            i1
          case 12 =>
            var keepGoing = true
            var i2 = i
            var id1 = id

            while (keepGoing) {
              val (i1, field) = readFieldBegin(i2, id1)

              i2 = i1
              id1 = field.id

              if (field.tpe == 0) {
                keepGoing = false
              }
              else i2 = skip(i1, field.id, field.tpe)
            }

            i2
          case 15 =>
            val (i1, tl) = readListBegin(i)
            var i2 = i1

            for (x <- 0 until tl.size) {
              i2 = skip(i2, 0, tl.elemType)
            }

            i2
          }

        skip(i, 0, tpe)
      }

      //      def isBoolean(byte: Byte): Boolean = {
      //        val l = byte & 15
      //        l == 1 || l == 2
      //      }

      def getType(t: Byte): Byte = (t & 15).asInstanceOf[Byte] match {
        case 0 | 3 | 12 => (t & 15).asInstanceOf[Byte]
        case 1 | 2 => 2
        case 4 => 6
        case 5 => 8
        case 6 => 10
        case 7 => 4
        case 8 => 11
        case 9 => 15
        case 10 => 14
        case 11 => 13
        case _ => throw new Error(s"Unable to match type ${(t & 15).asInstanceOf[Byte]}")
      }

      val (i1, field) = readFieldBegin(i, id)

      if (field == TSTOP) new ParquetMetadata(array.length, version, schema, numRows, rowGroups, createdBy)
      else field match {
        case TField(8, 1) =>
          val (i2, v) = readI32(i1)
          doRead(i2, 1)(v, schema, numRows, rowGroups, createdBy)
        case TField(15, 2) =>
          def readSchemaElements(i: Int, j: Int, acc: List[SchemaElement]): (Int, List[SchemaElement]) = {
            def readSchemaElement(i: Int, id: Int)(tpe: Int, typeLength: Int, fieldRepetitionType: Int, name: String, numChildren: Int, convertedType: Int, scale: Int, precision: Int, fieldId: Int): (Int, SchemaElement) = {
              val (i1, field) = readFieldBegin(i, id)

              if (field == TSTOP) (i1, new SchemaElement(null, tpe, typeLength, fieldRepetitionType, name, numChildren, convertedType, scale, precision, fieldId))
              else field match {
                case TField(8, x) =>
                  val (i2, v) = readI32(i1)

                  //val nX = if (x < 4) x else x - 1

                  x match {
                    case 1 =>
                      readSchemaElement(i2, x)(v, typeLength, fieldRepetitionType, name, numChildren, convertedType, scale, precision, fieldId)
                    case 2 =>
                      readSchemaElement(i2, x)(tpe, v, fieldRepetitionType, name, numChildren, convertedType, scale, precision, fieldId)
                    case 3 =>
                      readSchemaElement(i2, x)(tpe, typeLength, v, name, numChildren, convertedType, scale, precision, fieldId)
                    case 5 =>
                      readSchemaElement(i2, x)(tpe, typeLength, fieldRepetitionType, name, v, convertedType, scale, precision, fieldId)
                    case 6 =>
                      readSchemaElement(i2, x)(tpe, typeLength, fieldRepetitionType, name, numChildren, v, scale, precision, fieldId)
                    case 7 =>
                      readSchemaElement(i2, x)(tpe, typeLength, fieldRepetitionType, name, numChildren, convertedType, v, precision, fieldId)
                    case 8 =>
                      readSchemaElement(i2, x)(tpe, typeLength, fieldRepetitionType, name, numChildren, convertedType, scale, v, fieldId)
                    case 9 =>
                      readSchemaElement(i2, x)(tpe, typeLength, fieldRepetitionType, name, numChildren, convertedType, scale, precision, v)
                  }
                //readSchemaElement(i2, x, new SchemaElement(parent, v, typeLength, fieldRepetitionType, name, numChildren, convertedType, scale, precision, fieldId))
                case TField(11, 4) =>
                  val (i2, name) = readString(i1)
                  readSchemaElement(i2, 4)(tpe, typeLength, fieldRepetitionType, name, numChildren, convertedType, scale, precision, fieldId)
              }
            }

            if (j == 0) (i, acc)
            else {
              val (i1, se) = readSchemaElement(i, 0)(0, 0, 0, "", 0, 0, 0, 0, 0)
              readSchemaElements(i1, j - 1, acc :+ se)
            }
          }

          val (i2, tl) = readListBegin(i1)
          val (i3, schemaElements) = readSchemaElements(i2, tl.size, Nil)
          doRead(i3, 2)(version, schemaElements, numRows, rowGroups, createdBy)
        case TField(10, 3) =>
          val (i2, nr) = readI64(i1)
          doRead(i2, 3)(version, schema, nr, rowGroups, createdBy)
        case TField(15, 4) =>
          def readRowGroups(i: Int, j: Int, acc: List[RowGroup]): (Int, List[RowGroup]) = {
            def readRowGroup(i: Int, id: Int)(columns: List[ColumnChunk], totalByteSize: Long, numRows: Long): (Int, RowGroup) = {
              val (i1, field) = readFieldBegin(i, id)

              if (field == TSTOP) (i1, RowGroup(columns, totalByteSize, numRows))
              else field match {
                case TField(15, x) =>
                  val (i2, tl) = readListBegin(i1)

                  x match {
                    case 1 =>
                      def readColumnChunks(i: Int, j: Int, acc: List[ColumnChunk]): (Int, List[ColumnChunk]) = {
                        def readColumnChunk(i: Int, id: Int)(filePath: String, fileOffset: Long, dataStart: Int, pageHeader: PageHeader, metadata: ColumnMetadata): (Int, ColumnChunk) = {
                          val (i1, field) = readFieldBegin(i, id)

                          if (field == TSTOP) (i1, ColumnChunk(filePath, fileOffset, dataStart, pageHeader, metadata))
                          else field match {
                            case TField(11, 1) =>
                              val (i2, fp) = readString(i1)
                              readColumnChunk(i2, 1)(fp, fileOffset, dataStart, pageHeader, metadata)
                            case TField(10, 2) =>
                              def readPageHeader(i: Int): (Int, PageHeader) = {
                                def readPageHeader(i: Int, id: Int)(tpe: Int, uncompressedPageSize: Int, compressedPageSize: Int, dataPageHeader: DataPageHeader): (Int, PageHeader) = {
                                  val (i1, field) = readFieldBegin(i, id)

                                  if (field == TSTOP) (i1, PageHeader(tpe, uncompressedPageSize, compressedPageSize, dataPageHeader))
                                  else field match {
                                    case TField(8, y) =>
                                      val (i2, v) = readI32(i1)

                                      y match {
                                        case 1 => readPageHeader(i2, 1)(v, uncompressedPageSize, compressedPageSize, dataPageHeader)
                                        case 2 => readPageHeader(i2, 2)(tpe, v, compressedPageSize, dataPageHeader)
                                        case 3 => readPageHeader(i2, 3)(tpe, uncompressedPageSize, v, dataPageHeader)                                                                               
                                      }

                                    case TField(12, 5) =>
                                      def readDataPageHeader(i: Int): (Int, DataPageHeader) = {
                                        def readDataPageHeader(i: Int, id: Int)(numValues: Int, encoding: Int, definitionLevelEncoding: Int, repetitionLevelEncoding: Int): (Int, DataPageHeader) = {
                                          val (i1, field) = readFieldBegin(i, id)

                                          if (field == TSTOP) (i1, DataPageHeader(numValues, encoding, definitionLevelEncoding, repetitionLevelEncoding))
                                          else field match {
                                            case TField(8, y) =>
                                              val (i2, v) = readI32(i1)
                      
                                              y match {
                                                case 1 => readDataPageHeader(i2, 1)(v, encoding, definitionLevelEncoding, repetitionLevelEncoding)
                                                case 2 => readDataPageHeader(i2, 2)(numValues, v, definitionLevelEncoding, repetitionLevelEncoding)
                                                case 3 => readDataPageHeader(i2, 3)(numValues, encoding, v, repetitionLevelEncoding)
                                                case 4 => readDataPageHeader(i2, 4)(numValues, encoding, definitionLevelEncoding, v)
                                                case _ => 
                                                  val i3 = skip(i2, 8)
                                                  readDataPageHeader(i3, field.id)(numValues, encoding, definitionLevelEncoding, repetitionLevelEncoding)
                                              }

                                            case TField(12, 5) =>
                                              val i2 = skip(i1, 12)
                                              readDataPageHeader(i2, 5)(numValues, encoding, definitionLevelEncoding, repetitionLevelEncoding)
                                          }
                                        } //end readDataPageHeader

                                        readDataPageHeader(i, 0)(0, 0, 0, 0)
                                      }                                    

                                      val (i2, dph) = readDataPageHeader(i1)
                                      readPageHeader(i2, 5)(tpe, uncompressedPageSize, compressedPageSize, dph)

                                    case TField(12, 7) =>
                                      val i2 = skip(i1, 12)
                                      readPageHeader(i2, 7)(tpe, uncompressedPageSize, compressedPageSize, dataPageHeader)
                                  }
                                }

                                val (i1, ph) = readPageHeader(i, 0)(0, 0, 0, null)
                                val (i2, padding) = readInt(i1)
                                (i2 + padding, ph)
                              }

                              val (i2, fo) = readI64(i1)
                              val (dataStart, ph) = readPageHeader(fo.asInstanceOf[Int])

                              readColumnChunk(i2, 2)(filePath, fo, dataStart, ph, metadata)
                            case TField(12, 3) =>
                              def readColumnMetadata(i: Int, id: Int)(tpe: Int, encodings: List[Int], pathInSchema: List[String], codec: Int, numValues: Long, totalUncompressedSize: Long, totalCompressedSize: Long, dataPageOffset: Long, indexPageOffset: Long, dictionaryPageOffset: Long): (Int, ColumnMetadata) = {
                                val (i1, field) = readFieldBegin(i, id)

                                if (field == TSTOP) (i1, ColumnMetadata(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset))
                                else field match {
                                  case TField(8, y) =>
                                    val (i2, v) = readI32(i1)
                                    y match {
                                      case 1 => readColumnMetadata(i2, 1)(v, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset)
                                      case 4 => readColumnMetadata(i2, 4)(tpe, encodings, pathInSchema, v, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset)
                                    }
                                  case TField(15, y) =>
                                    val (i2, tl) = readListBegin(i1)

                                    y match {
                                      case 2 =>
                                        def readEncodings(i: Int, j: Int, acc: List[Int]): (Int, List[Int]) = {
                                          if (j == 0) (i, acc)
                                          else {
                                            val (i1, e) = readI32(i)
                                            readEncodings(i1, j - 1, acc :+ e)
                                          }
                                        }

                                        val (i3, encodings) = readEncodings(i2, tl.size, Nil)
                                        readColumnMetadata(i3, 2)(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset)
                                      case 3 =>
                                        def readPathInSchema(i: Int, j: Int, acc: List[String]): (Int, List[String]) = {
                                          if (j == 0) (i, acc)
                                          else {
                                            val (i1, pis) = readString(i)
                                            readPathInSchema(i1, j - 1, acc :+ pis)
                                          }
                                        }

                                        val (i3, pathInSchema) = readPathInSchema(i2, tl.size, Nil)
                                        readColumnMetadata(i3, 3)(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset)

                                      case 13 =>
                                        var i3 = i2

                                        for (z <- 0 until tl.size) {
                                          i3 = skip(i3, 12)
                                        }

                                        readColumnMetadata(i3, 13)(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset)
                                    }
                                  case TField(10, y) =>
                                    val (i2, v) = readI64(i1)

                                    y match {
                                      case 5  => readColumnMetadata(i2, 5)(tpe, encodings, pathInSchema, codec, v, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset)
                                      case 6  => readColumnMetadata(i2, 6)(tpe, encodings, pathInSchema, codec, numValues, v, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset)
                                      case 7  => readColumnMetadata(i2, 7)(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, v, dataPageOffset, indexPageOffset, dictionaryPageOffset)
                                      case 9  => readColumnMetadata(i2, 9)(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, v, indexPageOffset, dictionaryPageOffset)
                                      case 10 => readColumnMetadata(i2, 10)(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, v, dictionaryPageOffset)
                                      case 11 => readColumnMetadata(i2, 11)(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, v)
                                    }

                                  case TField(12, 12) =>
                                    val i2 = skip(i1, 12)
                                    readColumnMetadata(i2, 12)(tpe, encodings, pathInSchema, codec, numValues, totalUncompressedSize, totalCompressedSize, dataPageOffset, indexPageOffset, dictionaryPageOffset)
                                }
                              }

                              val (i2, metadata) = readColumnMetadata(i1, 0)(0, Nil, Nil, 0, 0, 0, 0, 0, 0, 0)
                              readColumnChunk(i2, 3)(filePath, fileOffset, dataStart, pageHeader, metadata)
                          }
                        }

                        if (j == 0) (i, acc)
                        else {
                          val (i2, cc) = readColumnChunk(i, 0)("", 0, 0, null, null)
                          readColumnChunks(i2, j - 1, acc :+ cc)
                        }
                      }

                      val (i3, columnChunks) = readColumnChunks(i2, tl.size, Nil)
                      readRowGroup(i3, 1)(columnChunks, totalByteSize, numRows)
                    case _ => throw new Error("")
                  }
                case TField(10, x) =>
                  val (i2, v) = readI64(i1)

                  x match {
                    case 2 => readRowGroup(i2, 2)(columns, v, numRows)
                    case 3 => readRowGroup(i2, 3)(columns, totalByteSize, v)
                  }
              }
            }

            if (j == 0) (i, acc)
            else {
              val (i2, rg) = readRowGroup(i, 0)(Nil, 0, 0)
              readRowGroups(i2, j - 1, acc :+ rg)
            }
          }

          val (i2, tl) = readListBegin(i1)
          val (i3, rgs) = readRowGroups(i2, tl.size, Nil)
          doRead(i3, 4)(version, schema, numRows, rgs, createdBy)
        case TField(15, 5) =>
          val i2 = skip(i1, 15)
          doRead(i2, 5)(version, schema, numRows, rowGroups, createdBy)
        case TField(11, 6) =>
          val (i2, cb) = readString(i1)
          doRead(i2, 6)(version, schema, numRows, rowGroups, cb)
      }
    }

    doRead(fileMetadataIndex, 0)(0, Nil, 0, Nil, "")
  }

  val array = Files.readAllBytes(Paths.get(path))

  val fileMetadataIndex: Int = {
    val l = array.length

    val footerLengthIndex = l - 4 - MAGIC.length
    val footerLength = {
      val x1 = array(footerLengthIndex) & 255
      val x2 = array(footerLengthIndex + 1) & 255
      val x3 = array(footerLengthIndex + 2) & 255
      val x4 = array(footerLengthIndex + 3) & 255

      if ((x1 | x2 | x3 | x4) < 0) throw new Error("Hit EOF early")

      (x4 << 24) + (x3 << 16) + (x2 << 8) + (x1 << 0)
    }

    val magic = new Array[Byte](MAGIC.length)

    for (i <- MAGIC.indices) {
      magic(i) = array(footerLengthIndex + 4 + i)
    }

    //if (magic.sameElements(MAGIC)) throw new Error("Not a Parquet File")

    val ret = footerLengthIndex - footerLength
    ret
  }

  case class TList(elemType: Byte, size: Int)
  case class TField(tpe: Byte, id: Short)
  object TSTOP extends TField(0, 0)
}

class ParquetMetadata(val totalSize: Int, val version: Int, val schema: List[SchemaElement], val numRows: Long, val rowGroups: List[RowGroup], val createdBy: String) {
  override def toString: String = s"Parquet metadata information:\nVersion: $version\nSchema:\n" + { schema.foreach(x => x.name + "\n") } + s"NumRows: $numRows\n# of RowGroups: ${rowGroups.length}\nCreated By: $createdBy\n\n"
}

class SchemaElement(val parent: SchemaElement, val tpe: Int, val typeLength: Int, val fieldRepetitionType: Int, val name: String, val numChildren: Int, val convertedType: Int, val scale: Int, val precision: Int, val fieldId: Int) {
  lazy val definition: Int = {
    if (fieldRepetitionType == 1 || fieldRepetitionType == 2)
      if (parent != null) parent.definition + 1
      else 1
    else 0
  }

  lazy val repetition: Int = {
    if (fieldRepetitionType == 2)
      if (parent != null) parent.repetition + 1
      else 1
    else 0
  }
  0
}

case class RowGroup(columns: List[ColumnChunk], totalByteSize: Long, numRows: Long)
case class PageHeader(tpe: Int, uncompressedPageSize: Int, compressedPageSize: Int, dataPageHeader: DataPageHeader)
case class DataPageHeader(numValues: Int, encoding: Int, definitionLevelEncoding: Int, repetitionLevelEncoding: Int)
case class ColumnChunk(filePath: String, fileOffset: Long, dataStart: Int, pageHeader: PageHeader, metadata: ColumnMetadata)
case class ColumnMetadata(tpe: Int, encodings: List[Int], pathInSchema: List[String], codec: Int, numValues: Long, totalUncompressedSize: Long, totalCompressedSize: Long, dataPageOffset: Long, indexPageOffset: Long, dictionaryPageOffset: Long)