package main.scala.Fauxquet.Encoders

import main.scala.Fauxquet.ValuesReaders.bitpacking.BytePacker_LE_1
import main.scala.Fauxquet.bytes.BytesInput.{BytesInput, BytesInputManager}
import main.scala.Fauxquet.bytes.CapacityByteArrayOutputStream

/**
  * Created by james on 1/25/17.
  */
class RunLengthBitPackingHybridEncoder(bitWidth: Int, capacityByteArrayOutputStream: CapacityByteArrayOutputStream) {
  val packBuffer = new Array[Byte](bitWidth)
  val bufferedValues = new Array[Int](8)

  var previousValue = 0
  var numBufferedValues = 0
  var repeatCount = 0
  var bitPackedGroupCount = 0
  var bitPackedRunHeaderPointer = -1
  var toBytesCalled = false

  def bufferedSize = capacityByteArrayOutputStream.size
  def allocatedSize = capacityByteArrayOutputStream.capacity

  def toBytes: BytesInput = {
    if (this.repeatCount >= 8) {
      writeRleRun()
    }
    else if (this.numBufferedValues > 0) {
      for (i <- this.numBufferedValues until 8) {
        this.bufferedValues(i) = 0
      }

      writeOrAppendBitPackedRun()
      endPreviousBitPackedRun()
    }
    else {
      endPreviousBitPackedRun()
    }

    toBytesCalled = true
    BytesInputManager.from(capacityByteArrayOutputStream)
  }

  def reset(resetCapacityByteArrayOutputStream: Boolean = true): Unit = {
    if (resetCapacityByteArrayOutputStream) {
      capacityByteArrayOutputStream.reset()
    }

    previousValue = 0
    numBufferedValues = 0
    repeatCount = 0
    bitPackedGroupCount = 0
    bitPackedRunHeaderPointer = -1
    toBytesCalled = false
  }

  def writeInt(value: Int): Unit = {
    if (value == previousValue) {
      repeatCount += 1

      if (repeatCount >= 8) return //handles rleRun
    }
    else {
      if (repeatCount >= 8) {
        writeRleRun()
      }

      repeatCount = 1
      previousValue = value
    }

    bufferedValues(numBufferedValues) = value
    numBufferedValues += 1

    if (numBufferedValues == 8) {
      writeOrAppendBitPackedRun()
    }
  }

  def writeOrAppendBitPackedRun(): Unit = {
    if (bitPackedGroupCount >= 63) {
      endPreviousBitPackedRun()
    }

    if (bitPackedRunHeaderPointer == -1) {
      capacityByteArrayOutputStream.write(0)
      bitPackedRunHeaderPointer = capacityByteArrayOutputStream.currentIndex
    }

    BytePacker_LE_1.pack8Values(bufferedValues, 0, packBuffer, 0)
    capacityByteArrayOutputStream.write(packBuffer)

    numBufferedValues = 0
    bitPackedGroupCount += 1
  }

  def writeRleRun(): Unit = {
    endPreviousBitPackedRun()

    capacityByteArrayOutputStream.writeUnsignedVarInt(repeatCount << 1)
    capacityByteArrayOutputStream.writeIntLittleEndianPaddedOnBitWidth(previousValue, bitWidth)

    repeatCount = 0
    numBufferedValues = 0
  }

  def endPreviousBitPackedRun(): Unit = {
    if (bitPackedRunHeaderPointer == -1) return //means we aren't in a run

    val bitPackHeader: Byte = ((bitPackedGroupCount << 1) | 1).asInstanceOf[Byte]
    capacityByteArrayOutputStream.setByte(bitPackedRunHeaderPointer, bitPackHeader)

    bitPackedRunHeaderPointer = -1
    bitPackedGroupCount = 0
  }

  def close(): Unit = {
    reset(resetCapacityByteArrayOutputStream = false)
    capacityByteArrayOutputStream.close()
  }
}
