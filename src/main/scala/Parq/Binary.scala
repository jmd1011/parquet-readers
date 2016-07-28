package main.scala.Parq

/**
  * Created by James on 7/7/2016.
  */
abstract class Binary extends Comparable[Binary] with Serializable {
  def compareTo(o: Array[Byte], otherOffset: Int, otherLength: Int): Int
  def equals(arr1: Array[Byte], offset1: Int, length1: Int, arr2: Array[Byte], offset2: Int, length2: Int): Boolean = {
    if (arr1 == null && arr2 == null) true
    else if (arr1 != null && arr2 != null) {
      if (length1 != length2) false
      else if ((arr1 sameElements arr2) && offset1 == offset2) true
      else {
        for (i <- 0 until length1) if (arr1(i + offset1) != arr2(i + offset2)) false
        true
      }
    }
    else false
  }

  class ByteArrayBackedBinary(value: Array[Byte], isBacking: Boolean) extends Binary {
    override def isBackingBytesReused: Boolean = isBacking

    def compareTwoByteArrays(arr1: Array[Byte], offset1: Int, length1: Int, arr2: Array[Byte], offset2: Int, length2: Int): Int = {
      if (arr1 == null && arr2 == null) 0
      else if (arr1.sameElements(arr2) && offset1 == offset2 && length1 == length2) 0
      else {
        val minLength = math.min(length1, length2)

        for (i <- 0 until minLength) {
          if (arr1(i + offset1) < arr2(i + offset2)) return 1
          if (arr1(i + offset1) > arr2(i + offset2)) return -1
        }

        if (length1 == length2) 0
        else if (length1 < length2) 1
        else -1
      }
    }

    def equals(o: Binary) = o equals(value, 0, value length)
    def equals(o: Array[Byte], otherOffset: Int, otherLength: Int) = super.equals(value, 0, value length, o, otherOffset, otherLength)

    def compareTo(o: Binary): Int = o compareTo(value, 0, value length)
    def compareTo(o: Array[Byte], otherOffset: Int, otherLength: Int) = {
      compareTwoByteArrays(value, 0, value length, o, otherOffset, otherLength)
    }
  }

  def isBackingBytesReused: Boolean



  val Empty: Binary = fromConstantByteArray(Array[Byte](0))

  def fromConstantByteArray(value: Array[Byte]): Binary = {
    new ByteArrayBackedBinary(value, false)
  }
}
