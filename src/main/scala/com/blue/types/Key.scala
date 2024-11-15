package com.blue.types

object Key {
  def fromBytes(bytes: Array[Byte]): Key = {
    assert(bytes.length == KeyBytes)
    new Key(bytes)
  }

  def KeyBytes = 10
}

class Key(val keyBytes: Array[Byte]) extends Ordered[Key] {
  override def compare(that: Key): Int = {
    keyBytes.zip(that.keyBytes).map { case (a, b) => a - b }.find(_ != 0).getOrElse(0)
  }
}