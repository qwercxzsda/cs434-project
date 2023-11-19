package com.blue.record_file_manipulator

import com.blue.proto.record._

import com.blue.check.Check

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import scala.io._
import com.typesafe.scalalogging.Logger

class RecordFileManipulator(inputDirectories: List[String], outputDirectory: String) {
  private val logger: Logger = Logger("RecordFileManipulator")
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

  logger.info(s"RecordFileManipulator instantiated with \ninputPath: $inputPath, \noutputPath: $outputPath, \ninputSortedPath: $inputSortedPath, \ndistributedPath: $distributedPath")

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
    // sampling is done on unsorted input file
    logger.info(s"Sampling $sampleNum records from $inputPath")

    val inputSource: BufferedSource = scala.io.Source.fromFile(inputPath)
    val inputIterator: Iterator[String] = inputSource.getLines()
    val samples: List[Record] = try {
      val samplesString: List[String] = inputIterator.take(sampleNum).toList
      samplesString map stringToRecord
    } finally {
      inputSource.close()
    }
    samples
  }

  def getRecordsToDistribute: (Iterator[Record], BufferedSource) = {
    logger.info(s"Obtaining records to distribute")
    sort(inputPath, inputSortedPath)
    val inputSortedSource: BufferedSource = scala.io.Source.fromFile(inputSortedPath)
    val inputSortedIterator: Iterator[String] = inputSortedSource.getLines()
    val recordsToDistribute: Iterator[Record] = inputSortedIterator map stringToRecord
    (recordsToDistribute, inputSortedSource)
  }

  def closeRecordsToDistribute(toClose: BufferedSource): Unit = {
    toClose.close()
  }

  def sortDistributedRecords(): Unit = {
    logger.info(s"Sorting distributed records")
    sort(distributedPath, outputPath)
  }

  def getSortResult: (Record, Record) = {
    // TODO: this implementation only works for data fitting in memory
    logger.info(s"Obtaining sort result")
    val outputSource: BufferedSource = scala.io.Source.fromFile(outputPath)
    val outputIterator: Iterator[String] = outputSource.getLines()
    try {
      val recordsString: List[String] = outputIterator.toList
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
    Check.weakAssertEq(logger)(key.length, keyLength, s"key.length is not equal to keyLength")
    Record(key, value)
  }
}
