import org.scalatest.funsuite.AnyFunSuite
import com.blue.master.Master
import com.blue.network.NetworkConfig
import com.blue.worker.Worker

class NetworkSuite extends AnyFunSuite {
  test("Smoke Test") {
    val masterThread = new MasterThread
    val workerThread1 = new WorkerThread(getClass.getResource("/data/1").getPath, "src/test/resources/output1")
    val workerThread2 = new WorkerThread(getClass.getResource("/data/2").getPath, "src/test/resources/output2")

    masterThread.start()
    Thread.sleep(1000)

    workerThread1.start()
    Thread.sleep(1000)

    NetworkConfig.workerPort += 1
    workerThread2.start()

    workerThread1.join()
    workerThread2.join()
    masterThread.join()
  }
}

class MasterThread extends Thread {
  override def run(): Unit = {
    Master.main(Array("2"))
  }
}

class WorkerThread(input: String, output: String) extends Thread {
  override def run(): Unit = {
    // Todo: change input to list
    new Worker(NetworkConfig.ip, NetworkConfig.workerPort, Array("127.0.0.1:30962", "-I", input, "-O", output))
  }
}