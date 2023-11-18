package com.blue.record_file

import com.blue.proto.record._

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import scala.io._

class RecordFile(inputDirectories: List[String], outputDirectory: String) {
  private val inputPath: String = inputDirectories.head + "/partition1"
  private val outputPath: String = outputDirectory + "/partition1"
  private val tempPath: String = outputDirectory + "/temp/partition1"

  Files.deleteIfExists(Paths.get(outputPath))
  Files.deleteIfExists(Paths.get(tempPath))

  //  private val outputWriter: FileWriter = new FileWriter(new File(outputPath))
  //  outputWriter.close()
  //  private val tempWriter: FileWriter = new FileWriter(new File(tempPath))
  //  tempWriter.close()
  //
  //  private val inputSource: BufferedSource = scala.io.Source.fromFile(inputPath)
  //  private val inputIterator: Iterator[String] = inputSource.getLines()
  //  inputSource.close()

  def saveDistributedRecords(records: Seq[Record]): Unit = {
    val tempWriter: FileWriter = new FileWriter(new File(tempPath), true)
    try {
      records foreach (record => tempWriter.write(record.key + record.value + "\n"))
    } finally {
      tempWriter.close()
    }
  }
}
