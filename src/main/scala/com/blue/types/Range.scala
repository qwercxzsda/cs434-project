package com.blue.types

class Range(val start: Key, val end: Key) {
  def contains(key: Key): Boolean = {
    start.compare(key) <= 0 &&  0 < end.compare(key)
  }
}