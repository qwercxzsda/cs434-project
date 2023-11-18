import org.scalatest.funsuite.AnyFunSuite
import com.blue.worker.core.Sampler

import java.io.{BufferedInputStream, FileInputStream}

class SamplerTest extends AnyFunSuite {
  test("Should not sample") {
    val limit0Samples = Sampler.sample(List(getClass.getResource("/Sampler").getPath), 0, 1f)
    assert(limit0Samples.isEmpty)
    val ratio0Samples = Sampler.sample(List(getClass.getResource("/Sampler").getPath), 100, 0f)
    assert(ratio0Samples.isEmpty)
  }

  test("Should sample") {
    val everySamples = Sampler.sample(List(getClass.getResource("/Sampler").getPath), 100, 1f)
    assert(everySamples.length == 6)
    val limitOnlySample1 = Sampler.sample(List(getClass.getResource("/Sampler").getPath), 3, 1f)
    assert(limitOnlySample1.length == 5)
  }
}