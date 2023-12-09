import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.PrivateMethodTester
import com.blue.proto.record.Record
import com.blue.worker.{RecordFileManipulator, RecordConfig}
import com.google.protobuf.ByteString
import com.blue.bytestring_ordering.ByteStringOrdering._
import scala.math.Ordered.orderingToOrdered
import scala.io.BufferedSource

class RecordFileManipulatorSuite extends AnyFunSuite with PrivateMethodTester {
  test("openFile ascii") {
    val decorateOpenFile: PrivateMethod[(BufferedSource, Iterator[Record])] =
      PrivateMethod[(BufferedSource, Iterator[Record])](Symbol("openFile"))

    val outputDir: String = "worker/src/test/resources/temp"
    val recordFileManipulator = new RecordFileManipulator(List(), outputDir)
    val inputPath: String = "worker/src/test/resources/data_simple/single_ascii"
    val (inputSource, inputIterator) = recordFileManipulator invokePrivate decorateOpenFile(inputPath)
    val records: List[Record] = inputIterator.toList
    inputSource.close()
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

  test("openFile binary") {
    val decorateOpenFile: PrivateMethod[(BufferedSource, Iterator[Record])] =
      PrivateMethod[(BufferedSource, Iterator[Record])](Symbol("openFile"))

    val outputDir: String = "worker/src/test/resources/temp"
    val recordFileManipulator = new RecordFileManipulator(List(), outputDir)
    val inputPath: String = "worker/src/test/resources/data_simple/single"
    val (inputSource, inputIterator) = recordFileManipulator invokePrivate decorateOpenFile(inputPath)
    val records: List[Record] = inputIterator.toList
    inputSource.close()
    assert(records.length == 1)
    val record = records.head
    println(s"record: $record")

    val byteArray: Array[Byte] = Array(
      0x4a, 0x69, 0x6d, 0x47, 0x72, 0x61, 0x79, 0x52, 0x49, 0x50, 0x65, 0x08, 0x00, 0x60, 0x83, 0x6b,
      0x39, 0x2c, 0xd5, 0x8b, 0x8f, 0x5e, 0x55, 0x96, 0x18, 0x55, 0xe7, 0x9b, 0x87, 0xf0, 0x22, 0x83,
      0xa4, 0x66, 0xb6, 0xaa, 0xb1, 0xf9, 0xe0, 0xca, 0xcf, 0x1e, 0x26, 0xb3, 0x29, 0x2a, 0xfd, 0x10,
      0x64, 0xbb, 0x18, 0xb5, 0x6a, 0xc0, 0x7d, 0x6f, 0x65, 0x6b, 0x1d, 0x2f, 0x43, 0x0d, 0x57, 0xbd,
      0xe7, 0xe4, 0x7d, 0x81, 0xf3, 0x6a, 0x6d, 0xd2, 0x67, 0x94, 0x8b, 0xbc, 0x23, 0x97, 0xbf, 0xe2,
      0x8c, 0x33, 0x4e, 0x4a, 0xd8, 0x2b, 0x8e, 0x70, 0x16, 0x62, 0x93, 0xcf, 0xaa, 0x01, 0x16, 0xbf,
      0xda, 0x3b, 0xb1, 0xab) map (_.toByte)
    val byteStringKey: ByteString = ByteString.copyFrom(byteArray.take(RecordConfig.keyLength))
    val byteStringValue: ByteString = ByteString.copyFrom(byteArray.drop(RecordConfig.keyLength))
    assert(record.key.toByteArray ++ record.value.toByteArray === byteArray)
    assert(record.key === byteStringKey)
    assert(record.value === byteStringValue)
  }

  test("MergeSortIterators") {
    val decorateMergeSortIterators: PrivateMethod[Iterator[Record]] =
      PrivateMethod[Iterator[Record]](Symbol("mergeSortIterators"))

    val outputDir: String = "worker/src/test/resources/temp"
    val recordFileManipulator = new RecordFileManipulator(List(), outputDir)
    val list1_ = List(1, 3, 4, 5, 7, 10)
    val list2_ = List(3, 6, 8, 9, 10)
    val list1 = list1_ map (i => Record(ByteString.copyFrom(Array(i.toByte)), ByteString.EMPTY))
    val list2 = list2_ map (i => Record(ByteString.copyFrom(Array(i.toByte)), ByteString.EMPTY))
    val iterator1: Iterator[Record] = list1.iterator
    val iterator2: Iterator[Record] = list2.iterator
    val iterator: Iterator[Record] = recordFileManipulator invokePrivate decorateMergeSortIterators(List(iterator1, iterator2))
    val records: List[Record] = iterator.toList
    println(s"list1: ${list1 map (_.key)}\nsize: ${list1.size}")
    println(s"list2: ${list2 map (_.key)}\nsize: ${list2.size}")
    println(s"records: ${records map (_.key)}\nsize: ${records.size}")
    assert(records === (list1 ++ list2).sortBy(_.key))
  }

  test("MergeSortIterators lazy?") {
    val decorateMergeSortIterators: PrivateMethod[Iterator[Record]] =
      PrivateMethod[Iterator[Record]](Symbol("mergeSortIterators"))

    val outputDir: String = "worker/src/test/resources/temp"
    val recordFileManipulator = new RecordFileManipulator(List(), outputDir)
    val list1 = List(1, 3, 4, 5, 7, 10)
    val list2 = List(3, 6, 8, 9, 10)
    val iterator1: Iterator[Record] = list1.iterator map { i => {
      println(s"i: $i")
      Record(ByteString.copyFrom(Array(i.toByte)), ByteString.EMPTY)
    }
    }
    val iterator2: Iterator[Record] = list2.iterator map { j => {
      println(s"j: $j")
      Record(ByteString.copyFrom(Array(j.toByte)), ByteString.EMPTY)
    }
    }
    val iterator: Iterator[Record] = recordFileManipulator invokePrivate decorateMergeSortIterators(List(iterator1, iterator2))
    val records: List[Record] = iterator.take(4).toList
    println(s"records: $records")
    assert(records === records.sortBy(_.key))
  }
}
