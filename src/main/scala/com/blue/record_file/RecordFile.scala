package com.blue.record_file

import com.blue.proto.record._

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import scala.io._

class RecordFile(inputDirectories: List[String], outputDirectory: String) {
  private val sampleNum: Int = 10
  private val keyLength: Int = 10
  private val valueLength: Int = 90

  private val inputPath: String = inputDirectories.head + "/partition1"
  private val outputPath: String = outputDirectory + "/partition1"
  private val inputSortedPath: String = outputDirectory + "/tmp1/partition1"
  private val distributedPath: String = outputDirectory + "/tmp2/partition1"

  Files.deleteIfExists(Paths.get(outputPath))
  Files.deleteIfExists(Paths.get(inputSortedPath))
  Files.deleteIfExists(Paths.get(distributedPath))

  //  private val outputWriter: FileWriter = new FileWriter(new File(outputPath))
  //  outputWriter.close()
  //  private val tempWriter: FileWriter = new FileWriter(new File(tempPath))
  //  tempWriter.close()
  //
  //  private val inputSource: BufferedSource = scala.io.Source.fromFile(inputPath)
  //  private val inputIterator: Iterator[String] = inputSource.getLines()
  //  inputSource.close()

  def saveDistributedRecords(records: Seq[Record]): Unit = {
    val distributedWriter: FileWriter = new FileWriter(new File(distributedPath), true)
    try {
      records foreach (record => distributedWriter.write(record.key + record.value + "\n"))
    } finally {
      distributedWriter.close()
    }
  }

  def getSamples: List[Record] = {
    // Must input sort when taking samples!!
    sort(inputPath, inputSortedPath)

    val inputSortedSource: BufferedSource = scala.io.Source.fromFile(inputSortedPath)
    val inputSortedIterator: Iterator[String] = inputSortedSource.getLines()
    val samples: List[Record] = try {
      val samplesString: List[String] = inputSortedIterator.take(sampleNum).toList
      samplesString map (line => {
        val key: String = line.substring(0, keyLength)
        val value: String = line.substring(keyLength)
        assert(key.length == keyLength)
        assert(value.length == valueLength)
        Record(key, value)
      })
    } finally {
      inputSortedSource.close()
    }
    samples
  }

  // sort files in input path and save in temp path
  def sort(inputPath: String, outputPath: String): Unit = {
    // TODO: this implementation only works for data fitting in memory
    val inputSource: BufferedSource = scala.io.Source.fromFile(inputPath)
    val inputIterator: Iterator[String] = inputSource.getLines()
    val records: List[String] = try {
      inputIterator.toList
    } finally {
      inputSource.close()
    }

    val sortedRecords: List[String] = records.sorted

    val outputWriter: FileWriter = new FileWriter(new File(outputPath))
    try {
      sortedRecords foreach (record => outputWriter.write(record + "\n"))
    } finally {
      outputWriter.close()
    }
  }
}
