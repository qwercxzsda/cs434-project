package com.blue.types

import com.google.protobuf.ByteString

object Record {
  def fromBytes(bytes: Array[Byte]): Record = {
    assert(bytes.length == RecordBytes)
    val (keyBytes, valueBytes) = bytes.splitAt(Key.KeyBytes)
    new Record(Key.fromBytes(keyBytes), valueBytes)
  }

  def fromMultipleBytes(bytes: Array[Byte]): List[Record] = {
    val (head, tail) = bytes.splitAt(Record.RecordBytes)
    Record.fromBytes(head) :: fromMultipleBytes(tail)
  }

  def fromByteString(bytes: ByteString): Record = {
    assert(bytes.size == RecordBytes)
    val keyBytes = bytes.substring(0, Key.KeyBytes).toByteArray
    val valueBytes = bytes.substring(Key.KeyBytes, bytes.size).toByteArray
    new Record(Key.fromBytes(keyBytes), valueBytes)
  }

  def RecordBytes = 100
}

class Record(val key: Key, val value: Array[Byte]) extends Ordered[Record] {
  override def compare(that: Record): Int = {
    key.compare(that.key)
  }

  def toBytes(): Array[Byte] = {
    Array.concat(key.keyBytes, value)
  }
}
