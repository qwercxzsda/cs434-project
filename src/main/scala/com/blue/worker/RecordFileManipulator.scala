package com.blue.worker

import com.blue.bytestring_ordering.ByteStringOrdering._
import com.blue.check.Check
import com.blue.proto.record._
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.io._

class RecordFileManipulator(inputDirectories: List[String], outputDirectory: String) {
  private val logger: Logger = Logger("RecordFileManipulator")

  private val inputPath: String = inputDirectories.head + "/partition1"
  private val outputPath: String = outputDirectory + "/partition1"
  private val inputSortedPath: String = outputDirectory + "/tmp1/partition1"
  private val distributedPath: String = outputDirectory + "/tmp2/partition1"

  Files.deleteIfExists(Paths.get(outputPath))
  Files.deleteIfExists(Paths.get(inputSortedPath))
  Files.deleteIfExists(Paths.get(distributedPath))

  logger.info(s"RecordFileManipulator instantiated")
  logger.info(s"inputPath: $inputPath")
  logger.info(s"outputPath: $outputPath")
  logger.info(s"inputSortedPath: $inputSortedPath")
  logger.info(s"distributedPath: $distributedPath")

  def saveDistributedRecords(records: Seq[Record]): Unit = {
    val file: File = new File(distributedPath)
    if (!file.exists) file.createNewFile
    val distributedWriter: FileWriter = new FileWriter(file, true)
    try {
      // TODO: is this too slow?
      records foreach (record => Files.write(Paths.get(distributedPath), record.key.toByteArray ++ record.value.toByteArray, StandardOpenOption.APPEND))
    } finally {
      distributedWriter.close()
    }
  }

  def getSamples: List[Record] = {
    // sampling is done on unsorted input file
    logger.info(s"Sampling ${RecordConfig.sampleNum} records from $inputPath")

    val (inputSource: BufferedSource, inputIterator: Iterator[Record]) = openFile(inputPath)
    val samples: List[Record] = try {
      inputIterator.take(RecordConfig.sampleNum).toList
    } finally {
      inputSource.close()
    }
    samples
  }

  def getRecordsToDistribute: (Iterator[Record], BufferedSource) = {
    logger.info(s"Obtaining records to distribute")
    sort(inputPath, inputSortedPath)
    val (inputSortedSource: BufferedSource, inputSortedIterator: Iterator[Record]) = openFile(inputSortedPath)
    (inputSortedIterator, inputSortedSource)
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
    val (outputSource: BufferedSource, outputIterator: Iterator[Record]) = openFile(outputPath)
    try {
      val records: List[Record] = outputIterator.toList
      (records.head, records.last)
    } finally {
      outputSource.close()
    }
  }

  // sort files in input path and save in output path
  private def sort(inputPath: String, outputPath: String): Unit = {
    // TODO: this implementation only works for data fitting in memory
    val (inputSource: BufferedSource, inputIterator: Iterator[Record]) = openFile(inputPath)
    val records: List[Record] = try {
      inputIterator.toList
    } finally {
      inputSource.close()
    }

    val sortedRecords: List[Record] = records.sortBy(_.key)

    val file: File = new File(outputPath)
    if (!file.exists) file.createNewFile
    val outputWriter: FileWriter = new FileWriter(file)
    try {
      // TODO: change this filewrite
      sortedRecords foreach (record => outputWriter.write(record + "\n"))
    } finally {
      outputWriter.close()
    }
  }

  private def openFile(fileName: String): (BufferedSource, Iterator[Record]) = {
    val inputSource: BufferedSource = scala.io.Source.fromFile(fileName, "ISO-8859-1")
    val inputIterator: Iterator[Record] = inputSource.grouped(RecordConfig.recordLength) map stringToRecord
    (inputSource, inputIterator)
  }

  private def stringToRecord(seqChar: Seq[Char]): Record = {
    val arrayByte: Array[Byte] = seqChar.map(_.toByte).toArray
    val key: ByteString = ByteString.copyFrom(arrayByte.take(RecordConfig.keyLength))
    val value: ByteString = ByteString.copyFrom(arrayByte.drop(RecordConfig.keyLength))
    Check.weakAssertEq(logger)(key.size(), RecordConfig.keyLength, s"key.length is not equal to keyLength")
    Check.weakAssertEq(logger)(value.size(), RecordConfig.valueLength, s"value.length is not equal to valueLength")
    Record(key, value)
  }
}
