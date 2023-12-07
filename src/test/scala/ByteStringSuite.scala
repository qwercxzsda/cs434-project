import org.scalatest.funsuite.AnyFunSuite
import com.google.protobuf.ByteString
import com.blue.bytestring_ordering.ByteStringOrdering._
import scala.math.Ordered.orderingToOrdered

class ByteStringSuite extends AnyFunSuite {
  test("byteArray to ByteString") {
    val byteArray: Array[Byte] = Array(65, 66, 97, 98) map (_.toByte)
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    println(s"byteString: $byteString")
  }
  test("Compare ByteString") {
    val byteArray1: Array[Byte] = Array(65, 66, 97, 98) map (_.toByte)
    val byteString1: ByteString = ByteString.copyFrom(byteArray1)
    val byteArray2: Array[Byte] = Array(65, 66, 97, 99) map (_.toByte)
    val byteString2: ByteString = ByteString.copyFrom(byteArray2)
    val byteArray3: Array[Byte] = Array(66) map (_.toByte)
    val byteString3: ByteString = ByteString.copyFrom(byteArray3)

    println(s"byteString1: $byteString1")
    println(s"byteString2: $byteString2")
    println(s"byteString3: $byteString3")

    assert(byteString1 < byteString2)
    assert(byteString2 < byteString3)
    assert(byteString1 < byteString3)
  }
}
