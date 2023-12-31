package com.blue.worker

import com.blue.bytestring_ordering.ByteStringOrdering._

import scala.math.Ordered.orderingToOrdered
import com.blue.check.Check
import com.blue.proto.record._
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger

import java.util.concurrent.atomic.AtomicInteger
import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.annotation.tailrec
import scala.io._
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}

class RecordFileManipulator(inputDirectories: List[String], outputDirectory: String) {
  private val logger: Logger = Logger("RecordFileManipulator")

  private val inputSortedDirectory: String = outputDirectory + File.separator + "tmp1"
  private val distributedDirectory: String = outputDirectory + File.separator + "tmp2"

  List(outputDirectory, inputSortedDirectory, distributedDirectory) foreach initializeDirectory

  private val inputPaths: List[String] = inputDirectories flatMap getPathsFromDirectory
  private val inputSortComplete: Future[List[Unit]] =
    Future.sequence(inputPaths map (path => Future {
      logger.info(s"Sorting $path started")
      sortAndSaveToDirectory(path, inputSortedDirectory)
      logger.info(s"Sorting $path completed")
    }))

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
  def getSamples: Future[List[Record]] = async {
    // wait is done in order to postpone sending register request until sorting is done
    // sorting takes a lot of resources, and grpc server might be affected
    await(inputSortComplete)
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
      logger.info(s"Sampled ${samples.length} records from $path")
      samples
    }
  }

  // must call closeRecordsToDistribute on each returned BufferedSource
  def getRecordsToDistribute: Future[List[(BufferedSource, Iterator[Record])]] = async {
    await(inputSortComplete)
    logger.info(s"Obtaining records to distribute")
    val inputSortedPaths: List[String] = getPathsFromDirectory(inputSortedDirectory)
    inputSortedPaths map openFile
  }

  def closeRecordsToDistribute(toClose: BufferedSource): Unit = {
    toClose.close()
  }

  def saveDistributedRecords(records: Seq[Record]): Unit = {
    saveRecordsToDirectory(distributedDirectory, records)
  }

  // Keeps track of the "number of times saveRecordsToDirectory is called for a given directory"
  private val savedHistory: Map[String, AtomicInteger] =
    Map(inputSortedDirectory -> new AtomicInteger(0),
      distributedDirectory -> new AtomicInteger(0),
      outputDirectory -> new AtomicInteger(0))

  private def saveRecordsToDirectory(directory: String, records: Seq[Record]): Unit = {
    Check.weakAssert(logger)(savedHistory.contains(directory), s"Directory $directory not found in savedHistory $savedHistory")
    val num: Int = savedHistory(directory).getAndIncrement()
    val file: File = new File(directory + File.separator + f"partition$num%010d")
    Check.weakAssert(logger)(!file.exists, s"File $file already exists")

    val recordsConcatenated: Array[Byte] =
      (records flatMap (record => record.key.toByteArray ++ record.value.toByteArray)).toArray
    Files.write(Paths.get(file.getPath), recordsConcatenated)
    logger.info(s"Saved ${records.length} records to $file")
  }

  def sortDistributedRecords(): Unit = {
    logger.info(s"Sorting distributed records")
    val distributedPaths = getPathsFromDirectory(distributedDirectory)
    val (bufferedSources: List[BufferedSource], iterators: List[Iterator[Record]]) =
      (distributedPaths map openFile).unzip
    val iteratorMerged: Iterator[Record] = mergeSortIterators(iterators)
    val iteratorInBlocks: Iterator[List[Record]] =
      iteratorMerged.grouped(RecordConfig.writeBlockNum) map (_.toList)
    try {
      iteratorInBlocks foreach (records => saveRecordsToDirectory(outputDirectory, records))
    } finally {
      bufferedSources foreach (_.close())
    }
  }

  private def mergeIterators(iter1: Iterator[Record], iter2: Iterator[Record]): Iterator[Record] = {
    val head1: Option[Record] = if (iter1.hasNext) Some(iter1.next()) else None
    val head2: Option[Record] = if (iter2.hasNext) Some(iter2.next()) else None
    (head1, head2) match {
      case (None, None) => Iterator()
      case (None, Some(record)) => Iterator(record) ++ iter2
      case (Some(record), None) => Iterator(record) ++ iter1
      case (Some(record1), Some(record2)) =>
        if (record1.key < record2.key)
          Iterator(record1) ++ mergeIterators(iter1, Iterator(record2) ++ iter2)
        else
          Iterator(record2) ++ mergeIterators(Iterator(record1) ++ iter1, iter2)
    }
  }

  @tailrec
  private def splitIterators(iterators: List[Iterator[Record]], left: List[Iterator[Record]],
                             right: List[Iterator[Record]]): (List[Iterator[Record]], List[Iterator[Record]]) = {
    iterators match {
      case Nil => (left, right)
      case iter :: Nil => (iter :: left, right)
      case iter1 :: iter2 :: tail => splitIterators(tail, iter1 :: left, iter2 :: right)
    }
  }

  private def mergeSortIterators(iterators: List[Iterator[Record]]): Iterator[Record] = {
    val (iter1: List[Iterator[Record]], iter2: List[Iterator[Record]]) =
      splitIterators(iterators, List(), List())
    (iter1, iter2) match {
      case (Nil, Nil) => Iterator()
      case (_, Nil) =>
        Check.weakAssert(logger)(iter1.length == 1, s"iter1.length is not equal to 1")
        iter1.head
      case (_, _) =>
        val (iter1_sorted: Iterator[Record], iter2_sorted: Iterator[Record]) =
          (mergeSortIterators(iter1), mergeSortIterators(iter2))
        mergeIterators(iter1_sorted, iter2_sorted)
    }
  }

  // sort the file of input path and save it in the output directory
  private def sortAndSaveToDirectory(inputPath: String, outputDirectory: String): Unit = {
    val (inputSource: BufferedSource, inputIterator: Iterator[Record]) = openFile(inputPath)
    try {
      val iteratorInBlock: Iterator[List[Record]] =
        inputIterator.grouped(RecordConfig.writeBlockNum) map (_.toList)
      iteratorInBlock foreach (records => saveRecordsToDirectory(outputDirectory, records.sortBy(_.key)))
    } finally {
      inputSource.close()
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
