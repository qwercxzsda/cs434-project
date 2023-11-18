package com.blue.record_file_manipulator

import com.blue.proto.record._

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import scala.io._

class RecordFileManipulator(inputDirectories: List[String], outputDirectory: String) {
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

  println(s"RecordFileManipulator instantiated with \ninputPath: $inputPath, \noutputPath: $outputPath, \ninputSortedPath: $inputSortedPath, \ndistributedPath: $distributedPath")

  def saveDistributedRecords(records: Seq[Record]): Unit = {
    val file: File = new File(distributedPath)
    if (!file.exists) file.createNewFile
    val distributedWriter: FileWriter = new FileWriter(file, true)
    try {
      records foreach (record => distributedWriter.write(record.key + record.value + "\n"))
    } finally {
      distributedWriter.close()
    }
  }

  def getSamples: List[Record] = {
    // Must input sort when taking samples!!
    sort(inputPath, inputSortedPath)
    println(s"RecordFileManipulator.getSamples: input sorted")

    val inputSortedSource: BufferedSource = scala.io.Source.fromFile(inputSortedPath)
    val inputSortedIterator: Iterator[String] = inputSortedSource.getLines()
    val samples: List[Record] = try {
      val samplesString: List[String] = inputSortedIterator.take(sampleNum).toList
      samplesString map stringToRecord
    } finally {
      inputSortedSource.close()
    }
    println(s"RecordFileManipulator.getSamples: samples picked")
    samples
  }

  def getRecordsToDistribute: (Iterator[Record], BufferedSource) = {
    val inputSortedSource: BufferedSource = scala.io.Source.fromFile(inputSortedPath)
    val inputSortedIterator: Iterator[String] = inputSortedSource.getLines()
    val recordsToDistribute: Iterator[Record] = inputSortedIterator map stringToRecord
    println(s"RecordFileManipulator.getRecordsToDistribute: records to distribute obtained")
    (recordsToDistribute, inputSortedSource)
  }

  def closeRecordsToDistribute(toClose: BufferedSource): Unit = {
    toClose.close()
  }

  def sortDistributedRecords(): Unit = {
    println(s"RecordFileManipulator.sortDistributedRecords: sorting distributed records")
    sort(distributedPath, outputPath)
  }

  def getSortResult: (Record, Record) = {
    // TODO: this implementation only works for data fitting in memory
    val outputSource: BufferedSource = scala.io.Source.fromFile(outputPath)
    val outputIterator: Iterator[String] = outputSource.getLines()
    try {
      val recordsString: List[String] = outputIterator.toList
      println(s"RecordFileManipulator.getSortResult: sort result obtained")
      (stringToRecord(recordsString.head), stringToRecord(recordsString.last))
    } finally {
      outputSource.close()
    }
  }

  // sort files in input path and save in output path
  private def sort(inputPath: String, outputPath: String): Unit = {
    // TODO: this implementation only works for data fitting in memory
    val inputSource: BufferedSource = scala.io.Source.fromFile(inputPath)
    val inputIterator: Iterator[String] = inputSource.getLines()
    val records: List[String] = try {
      inputIterator.toList
    } finally {
      inputSource.close()
    }

    val sortedRecords: List[String] = records.sorted

    val file: File = new File(outputPath)
    if (!file.exists) file.createNewFile
    val outputWriter: FileWriter = new FileWriter(file)
    try {
      sortedRecords foreach (record => outputWriter.write(record + "\n"))
    } finally {
      outputWriter.close()
    }
  }

  private def stringToRecord(string: String): Record = {
    val key: String = string.substring(0, keyLength)
    val value: String = string.substring(keyLength)
    assert(key.length == keyLength, s"key length is ${key.length}, not $keyLength")
    Record(key, value)
  }
}
