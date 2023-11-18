package com.blue.worker.core

import com.blue.types.Record

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}

object Merger {
  // Merge sorted files into one sorted file
  def mergeSortedFiles(inputFilePaths: Seq[String], outputFilePath: String): Unit = {
    val outputStream = new BufferedOutputStream(new FileOutputStream(outputFilePath))
    val inputStreams = inputFilePaths.map(path => new BufferedInputStream(new FileInputStream(path)))

    var heap = inputStreams.zipWithIndex.map { case (stream, index) => {
        val bytes = stream.readNBytes(Record.RecordBytes)
        (index, Record.fromBytes(bytes))
      }
    }

    // Read the smallest record from the heap and write it to the output file
    while (heap.nonEmpty) {
      val (index, minVal) = heap.minBy(_._2)
      outputStream.write(minVal.toBytes())

      val bytesRead = inputStreams(index).readNBytes(Record.RecordBytes)

      // check the cursor of the file, if it is not the end of the file, update the heap
      if (bytesRead.nonEmpty) {
        heap = heap.updated(index, (index, Record.fromBytes(bytesRead)))
      } else {
        heap = heap.filterNot(_._1 == index)
      }
    }

    outputStream.close()
    inputStreams.foreach(_.close())
  }
}
