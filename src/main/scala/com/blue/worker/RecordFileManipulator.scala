package com.blue.worker

import com.blue.bytestring_ordering.ByteStringOrdering._
import com.blue.check.Check
import com.blue.proto.record._
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger

import java.util.concurrent.atomic.AtomicInteger
import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.io._

class RecordFileManipulator(inputDirectories: List[String], outputDirectory: String) {
  private val logger: Logger = Logger("RecordFileManipulator")

  private val inputSortedDirectory: String = outputDirectory + File.separator + "tmp1"
  private val distributedDirectory: String = outputDirectory + File.separator + "tmp2"

  List(outputDirectory, inputSortedDirectory, distributedDirectory) foreach initializeDirectory

  private val inputPaths: List[String] = inputDirectories flatMap getPathsFromDirectory

  logger.info(s"RecordFileManipulator instantiated")
  logger.info(s"inputDirectories: $inputDirectories")
  logger.info(s"outputDirectory: $outputDirectory")
  logger.info(s"inputSortedDirectory: $inputSortedDirectory")
  logger.info(s"distributedDirectory: $distributedDirectory")

  private def initializeDirectory(directoryName: String): Unit = {
    val directory: File = new File(directoryName)
    if (!directory.exists) directory.mkdirs
    Check.weakAssert(logger)(directory.isDirectory, s"$directoryName is not a directory")
    directory.listFiles() foreach { file => if (!file.isDirectory) file.delete() }
  }

  private def getPathsFromDirectory(directoryName: String): List[String] = {
    val directory: File = new File(directoryName)
    Check.weakAssert(logger)(directory.exists, s"$directoryName does not exist")
    Check.weakAssert(logger)(directory.isDirectory, s"$directoryName is not a directory")
    val files: List[File] = directory.listFiles().toList
    files map (_.getPath)
  }

  // sampling is done on unsorted input file
  def getSamples: List[Record] = {
    if (inputPaths.isEmpty) {
      logger.info(s"No Paths to sample from")
      List()
    }
    else {
      val path: String = inputPaths.head
      logger.info(s"Sampling ${RecordConfig.sampleNum} records from $path")

      val (inputSource: BufferedSource, inputIterator: Iterator[Record]) = openFile(path)
      val samples: List[Record] = try {
        inputIterator.take(RecordConfig.sampleNum).toList
      } finally {
        inputSource.close()
      }
      samples
    }
  }

  def saveDistributedRecords(records: Seq[Record]): Unit = {
    saveRecords(distributedDirectory, records)
  }

  // Keeps track of the "number of times saveRecords is called for a given directory"
  private val savedHistory: Map[String, AtomicInteger] =
    Map(inputSortedDirectory -> new AtomicInteger(0), distributedDirectory -> new AtomicInteger(0))

  private def saveRecords(directory: String, records: Seq[Record]): Unit = {
    Check.weakAssert(logger)(savedHistory.contains(directory), s"Directory $directory not found in savedHistory $savedHistory")
    val num: Int = savedHistory(directory).getAndIncrement()
    val file: File = new File(directory + File.separator + num)
    Check.weakAssert(logger)(!file.exists, s"File $file already exists")

    val recordsConcatenated: Array[Byte] = (records foldLeft Array[Byte]()) {
      (acc, record) => acc ++ record.key.toByteArray ++ record.value.toByteArray
    }
    Files.write(Paths.get(file.getPath), recordsConcatenated)
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
