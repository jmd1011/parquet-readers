package main.scala.Fauxquet.bytes

import java.io.OutputStream
import java.nio.ByteBuffer

/**
  * Created by james on 1/25/17.
  */
class CapacityByteArrayOutputStream(var initialSlabSize: Int, val maxCapacityHint: Int = 1024 * 1024, val byteBufferAllocator: ByteBufferAllocator = HeapByteBufferAllocator) extends OutputStream {
  var bytesUsed: Int = 0
  var bytesAllocated: Int = 0
  var currentSlabIndex: Int = 0

  var slabs: List[ByteBuffer] = List[ByteBuffer]()
  var currentSlab: ByteBuffer = ByteBuffer.wrap(new Array[Byte](0))

  def addSlab(minSize: Int) = {
    val nextSlabSize1: Int = {
      if (bytesUsed == 0) initialSlabSize
      else if (bytesUsed > maxCapacityHint / 5) maxCapacityHint / 5
      else bytesUsed
    }

    val nextSlabSize = math.max(nextSlabSize1, minSize)

    this.currentSlab = byteBufferAllocator.allocate(nextSlabSize)
    this.slabs :+= currentSlab
    this.bytesAllocated += nextSlabSize
    this.currentSlabIndex = 0
  }

  def write(b: Int): Unit = {
    if (!currentSlab.hasRemaining) {
      addSlab(1)
    }

    currentSlab.put(currentSlabIndex, b.asInstanceOf[Byte])
    currentSlabIndex += 1
    currentSlab.position(currentSlabIndex)
    bytesUsed += 1
  }

  override def write(b: Array[Byte], offset: Int, length: Int): Unit = {
    if (offset < 0 || offset > b.length || length < 0 || offset + length - b.length > 0) {
      throw new Error("Bad sizing in CBAOS.write")
    }

    if (length >= currentSlab.remaining()) {
      val rem = currentSlab.remaining()
      currentSlab.put(b, offset, rem)
      bytesUsed += rem
      currentSlabIndex += rem

      val overflow = length - rem
      addSlab(overflow)
      currentSlab.put(b, offset + rem, overflow)
      currentSlabIndex = overflow
      bytesUsed += overflow
    }
    else {
      currentSlab.put(b, offset, length)
      currentSlabIndex += length
      bytesUsed += length
    }
  }

  def writeToOutput(out: OutputStream, buf: ByteBuffer, length: Int): Unit = {
    if (buf.hasArray) {
      out.write(buf.array(), buf.arrayOffset(), length)
    }
    else {
      val copy = new Array[Byte](length)
      buf.flip()
      buf.get(copy)
      out.write(copy)
    }
  }

  def writeTo(out: OutputStream): Unit = {
    for (i <- 0 until slabs.size - 1) {
      writeToOutput(out, slabs(i), slabs(i).position())
    }

    writeToOutput(out, currentSlab, currentSlabIndex)
  }

  def reset(): Unit = {
    initialSlabSize = math.max(initialSlabSize, bytesUsed / 7)

    for (slab <- slabs) {
      byteBufferAllocator.release(slab)
    }

    currentSlabIndex = 0
    currentSlab.position(0)
    slabs = List[ByteBuffer]()
    currentSlab = ByteBuffer.wrap(new Array[Byte](0))
    bytesUsed = 0
    bytesAllocated = 0
  }

  def currentIndex = bytesUsed - 1
  def size = bytesUsed
  def capacity = bytesAllocated

  def setByte(index: Long, value: Byte) = {
    if (index >= bytesUsed) throw new Error("index too large in CBAOS.setByte")

    var seen = 0L

    for (slab <- slabs) {
      if (index < seen + slab.limit()) {
        slab.put((index - seen).asInstanceOf[Int], value)
      }

      seen += slab.limit()
    }
  }


  //These should probably go somewhere else -- in BytesUtils in Parquet, but only ever used with a CapacityByteArrayOutputStream
  def writeUnsignedVarInt(v: Int): Unit = {
    var value = v

    while ((value & 0xFFFFFF80) != 0L) {
      this.write((value & 0x7F) | 0x80)
      value >>>= 7
    }

    this.write(value & 0x7F)
  }

  def writeIntLittleEndianPaddedOnBitWidth(v: Int, bitWidth: Int): Unit = {
    val bytesWidth = (bitWidth + 7) / 8

    bytesWidth match {
      case 0 =>
      case 1 =>
        this.write((v >>> 0) & 0xFF)
      case 2 =>
        this.write((v >>> 0) & 0xFF)
        this.write((v >>> 8) & 0xFF)
      case 3 =>
        this.write((v >>> 0) & 0xFF)
        this.write((v >>> 8) & 0xFF)
        this.write((v >>> 16) & 0xFF)
      case 4 =>
        this.write((v >>> 0) & 0xFF)
        this.write((v >>> 8) & 0xFF)
        this.write((v >>> 16) & 0xFF)
        this.write((v >>> 24) & 0xFF)
      case _ => throw new Error("bytesWidth should be 0 - 4")
    }
  }
}
