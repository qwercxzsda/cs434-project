import org.scalatest.funsuite.AnyFunSuite

import com.blue.worker.core.Merger
import java.io.{BufferedInputStream, FileInputStream}

class MergerTest extends AnyFunSuite {
  test("Smoke Test") {
    val merger = Merger.mergeSortedFiles(List(getClass.getResource("/Merger/sample1").getPath, getClass.getResource("/Merger/sample2").getPath), "output")

    // compare output file with getClass.getResource("/sample1").getPath with byte by byte comparison
    val testResult = new BufferedInputStream(new FileInputStream("output")).readAllBytes()
    val sampleResult = new BufferedInputStream(new FileInputStream(getClass.getResource("/Merger/merged").getPath)).readAllBytes()

    assert(testResult.sameElements(sampleResult))

    // delete output file
    new java.io.File("output").delete()
  }
}