package com.blue.worker.core

import com.blue.types.{Key, Record}

import java.io.File
import java.io.RandomAccessFile
import scala.util.Random

object Sampler {
  private def sample(filePath: String, readCount: Int, ratio: Float): List[Key] = {
    val file = new RandomAccessFile(filePath, "r")
    val keys = scala.collection.mutable.ListBuffer[Key]()

    try {
      var filePointer = 0L
      val fileSize = file.length()
      var count = 0

      while (filePointer < fileSize && count < readCount) {
        file.seek(filePointer)
        val buffer = new Array[Byte](10)

        if (Random.nextFloat() < ratio) {
          file.read(buffer)
          keys += Key.fromBytes(buffer)
          count += 1
        }

        filePointer += Record.RecordBytes
      }
    } finally {
      file.close()
    }

    keys.toList
  }

  def sample(directories: List[String], limit: Int, ratio: Float): List[Key] = {
    directories.flatMap(directory => {
      val files = new File(directory).listFiles().filter(_.isFile).map(_.getPath)
      files.flatMap(file => sample(file, limit, ratio))
    })
  }
}
