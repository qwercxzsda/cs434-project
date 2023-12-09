package com.blue.worker

object RecordConfig {
  val sampleNum: Int = 1000       // 1000 * 100 B = 100 KB
  val keyLength: Int = 10
  val valueLength: Int = 90
  val recordLength: Int = keyLength + valueLength
  val writeBlockNum: Int = 20 * 1000
  val writeBlockSize: Int = writeBlockNum * recordLength  // 20 * 1000 * 100 B = 2 MB
}
