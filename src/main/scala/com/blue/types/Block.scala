package com.blue.types

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}

// Block = List[Record] & File
object Block {
  def fromFile(path: String): Block = {
    val inputStream = new BufferedInputStream(new FileInputStream(path))
    val block = Block.fromBytes(inputStream.readAllBytes())
    inputStream.close()

    block
  }

  private def fromBytes(bytes: Array[Byte]): Block = {
    Block.fromRecords(Record.fromMultipleBytes(bytes))
  }

  def fromRecords(records: List[Record]): Block = {
    new Block(records)
  }
}

class Block(val records: List[Record]) {
  def write(path: String): Unit = {
    val outputStream = new BufferedOutputStream(new FileOutputStream(path))
    outputStream.write(toBytes())
    outputStream.close()
  }

  def filter(range: Range): Block = {
    new Block(records.filter(record => range.contains(record.key)))
  }

  def filter(ranges: List[Range]): List[Block] = {
    ranges.map(range => filter(range))
  }

  def sort(): Block = {
    new Block(records.sortBy(record => record.key))
  }

  def toBytes(): Array[Byte] = {
    records.flatMap(_.toBytes()).toArray
  }
}
