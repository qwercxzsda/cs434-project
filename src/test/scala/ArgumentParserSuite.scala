import org.scalatest.funsuite.AnyFunSuite

import com.blue.worker.ArgumentParser

class ArgumentParserSuite extends AnyFunSuite {
  test("parse") {
    val args: List[String] = List("-I", "input1", "input2", "-O", "output1", "output2")
    val map: Map[String, List[String]] = ArgumentParser.parse(args)
    assert(map("-I") === List("input1", "input2"))
    assert(map("-O") === List("output1", "output2"))
    assert(map === Map("-I" -> List("input1", "input2"), "-O" -> List("output1", "output2")))
  }
  test("parse Worker style") {
    val args: List[String] = List("192.1.1.1:4000", "-I", "input/dir/1", "input/dir/2", "-O", "output/dir/1")
    val map: Map[String, List[String]] = ArgumentParser.parse(args)

    assert(map("") === List("192.1.1.1:4000"))
    assert(map("-I") === List("input/dir/1", "input/dir/2"))
    assert(map("-O") === List("output/dir/1"))
    assert(map === Map("" -> List("192.1.1.1:4000"),
      "-I" -> List("input/dir/1", "input/dir/2"), "-O" -> List("output/dir/1")))
  }
}
