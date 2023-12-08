import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.PrivateMethodTester
import com.blue.proto.record.Record
import com.blue.worker.{RecordFileManipulator, RecordConfig}
import com.google.protobuf.ByteString

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
}