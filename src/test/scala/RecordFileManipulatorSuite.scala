import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.PrivateMethodTester
import com.blue.proto.record.Record
import com.blue.worker.{RecordFileManipulator, RecordConfig}
import com.google.protobuf.ByteString
import com.blue.bytestring_ordering.ByteStringOrdering._
import scala.math.Ordered.orderingToOrdered
import scala.io.BufferedSource

class RecordFileManipulatorSuite extends AnyFunSuite with PrivateMethodTester {
  test("openFile") {
    val decorateOpenFile: PrivateMethod[(BufferedSource, Iterator[Record])] =
      PrivateMethod[(BufferedSource, Iterator[Record])](Symbol("openFile"))

    val outputDir: String = "src/test/resources/temp"
    val recordFileManipulator = new RecordFileManipulator(List("."), outputDir)
    val inputPath: String = "src/test/resources/data_simple/single_ascii"
    val (inputSource, inputIterator) = recordFileManipulator invokePrivate decorateOpenFile(inputPath)
    val records: List[Record] = inputIterator.toList
    assert(records.length == 1)
    val record = records.head
    println(s"record: $record")

    val byteArray: Array[Byte] = Array(
      0x41, 0x73, 0x66, 0x41, 0x47, 0x48, 0x4d, 0x35, 0x6f, 0x6d, 0x20, 0x20, 0x30, 0x30, 0x30, 0x30,
      0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
      0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x20, 0x20, 0x30, 0x30,
      0x30, 0x30, 0x32, 0x32, 0x32, 0x32, 0x30, 0x30, 0x30, 0x30, 0x32, 0x32, 0x32, 0x32, 0x30, 0x30,
      0x30, 0x30, 0x32, 0x32, 0x32, 0x32, 0x30, 0x30, 0x30, 0x30, 0x32, 0x32, 0x32, 0x32, 0x30, 0x30,
      0x30, 0x30, 0x32, 0x32, 0x32, 0x32, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x31, 0x31,
      0x31, 0x31, 0x0d, 0x0a) map (_.toByte)
    val byteStringKey: ByteString = ByteString.copyFrom(byteArray.take(RecordConfig.keyLength))
    val byteStringValue: ByteString = ByteString.copyFrom(byteArray.drop(RecordConfig.keyLength))
    assert(record.key.toByteArray ++ record.value.toByteArray === byteArray)
    assert(record.key === byteStringKey)
    assert(record.value === byteStringValue)
  }

  test("MergeSortIterators") {
    val decorateMergeSortIterators: PrivateMethod[Iterator[Record]] =
      PrivateMethod[Iterator[Record]](Symbol("mergeSortIterators"))

    val outputDir: String = "src/test/resources/temp"
    val recordFileManipulator = new RecordFileManipulator(List("."), outputDir)
    val list1 = List(1, 3, 4, 5, 7, 10)
    val list2 = List(3, 6, 8, 9, 10)
    val iterator1: Iterator[Record] = list1.iterator map (i => Record(ByteString.copyFrom(Array(i.toByte)), ByteString.EMPTY))
    val iterator2: Iterator[Record] = list2.iterator map (i => Record(ByteString.copyFrom(Array(i.toByte)), ByteString.EMPTY))
    val iterator: Iterator[Record] = recordFileManipulator invokePrivate decorateMergeSortIterators(List(iterator1, iterator2))
    val records: List[Record] = iterator.toList
    println(s"records: $records")
    assert(records === records.sortBy(_.key))
  }
  //  test("MergeSortIterators") {
  //    val decorateMergeSortIterators: PrivateMethod[Iterator[Record]] =
  //      PrivateMethod[Iterator[Record]](Symbol("mergeSortIterators"))
  //
  //    val outputDir: String = "src/test/resources/temp"
  //    val recordFileManipulator = new RecordFileManipulator(List("."), outputDir)
  //    val inputPath1: String = "src/test/resources/data_simple/single_ascii"
  //    val inputPath2: String = "src/test/resources/data_simple/single_ascii"
  //    val (inputSource1, inputIterator1) = recordFileManipulator.openFile(inputPath1)
  //    val (inputSource2, inputIterator2) = recordFileManipulator.openFile(inputPath2)
  //    val iterator1: Iterator[Record] = inputIterator1
  //    val iterator2: Iterator[Record] = inputIterator2
  //    val iterator: Iterator[Record] = recordFileManipulator invokePrivate decorateMergeSortIterators(List(iterator1, iterator2))
  //    val records: List[Record] = iterator.toList
  //    assert(records.length == 2)
  //    val record1 = records.head
  //    val record2 = records.tail.head
  //  }
}
