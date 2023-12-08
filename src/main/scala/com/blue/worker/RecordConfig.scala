package com.blue.worker

object RecordConfig {
  val sampleNum: Int = 10
  val keyLength: Int = 10
  val valueLength: Int = 90
  val recordLength: Int = keyLength + valueLength
  val writeBlockSize: Int = 24 * 1000 * 10      // 24 * 1000 * 10 * 100 B = 24 MB
}
