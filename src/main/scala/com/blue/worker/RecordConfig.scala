package com.blue.worker

object RecordConfig {
  val sampleNum: Int = 100 * 10     // 100 * 10 * 100 B = 100 KB
  val keyLength: Int = 10
  val valueLength: Int = 90
  val recordLength: Int = keyLength + valueLength
  val writeBlockNum: Int = 24 * 1000 * 10
  val writeBlockSize: Int = writeBlockNum * recordLength  // 24 * 1000 * 10 * 100 B = 24 MB
}
