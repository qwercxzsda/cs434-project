package com.blue.master

import com.blue.proto.record._
import com.blue.proto.register._
import com.blue.proto.distribute._
import com.blue.proto.sort._
import com.blue.proto.master._
import com.blue.proto.worker._

import com.blue.network.NetworkConfig
import com.blue.check.Check

import com.google.protobuf.ByteString
import io.grpc.{Server, ServerBuilder}
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.util.{Failure, Success}
import scala.concurrent.Await
import scala.concurrent.duration._

object Master extends App {
  private val workerNum: Int = args(0).toInt

  private val registerRequests: ConcurrentLinkedQueue[RegisterRequest] = new ConcurrentLinkedQueue[RegisterRequest]()
  private val registerAllComplete: Promise[Unit] = Promise()
  private val workerIps: Future[List[String]] = getWorkerIps
  private val ranges: Future[List[String]] = getRanges

  sendDistributeStart

  private val distributeCompleteRequests: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
  private val distributeCompleteAllComplete: Promise[Unit] = Promise()
  private val distributeCompleteWorkerIps: Future[List[String]] = getDistributeCompleteWorkerIps

  sendSortStart

  private val sortCompleteRequests: ConcurrentLinkedQueue[SortCompleteRequest] = new ConcurrentLinkedQueue[SortCompleteRequest]()
  private val sortCompleteAllComplete: Promise[Unit] = Promise()

  private val server = ServerBuilder.
    forPort(NetworkConfig.port).
    addService(MasterGrpc.bindService(new MasterImpl, ExecutionContext.global)).
    build.start

  // TODO: verify sort results, use logging
  /* All the code above executes asynchronously.
   * As as result, this part of code is reached immediately.
   */
  println(s"Master server started at ${NetworkConfig.ip}:${NetworkConfig.port}")
  private val result: Unit = Await.result(sortCompleteAllComplete.future, Duration.Inf)


  private class MasterImpl extends MasterGrpc.Master {
    override def register(request: RegisterRequest): Future[RegisterResponse] = {
      registerRequests add request
      if (registerRequests.size >= workerNum) {
        assert(registerRequests.size == workerNum, s"registerRequests.size is ${registerRequests.size}, not $workerNum")
        registerAllComplete trySuccess ()
      }
      println(s"Master received register request from ${request.ip}")
      Future(RegisterResponse(ip = NetworkConfig.ip, success = true))
    }

    override def distributeComplete(request: DistributeCompleteRequest): Future[DistributeCompleteResponse] = {
      distributeCompleteRequests add request.ip
      if (distributeCompleteRequests.size >= workerNum) {
        assert(distributeCompleteRequests.size == workerNum, s"distributeCompleteRequests.size is ${distributeCompleteRequests.size}, not $workerNum")
        distributeCompleteAllComplete trySuccess ()
      }
      println(s"Master received distribute complete request from ${request.ip}")
      Future(DistributeCompleteResponse(success = true))
    }

    override def sortComplete(request: SortCompleteRequest): Future[SortCompleteResponse] = {
      sortCompleteRequests add request
      if (sortCompleteRequests.size >= workerNum) {
        assert(sortCompleteRequests.size == workerNum, s"sortCompleteRequests.size is ${sortCompleteRequests.size}, not $workerNum")
        sortCompleteAllComplete trySuccess ()
      }
      println(s"Master received sort complete request from ${request.ip}")
      Future(SortCompleteResponse(success = true))
    }
  }

  private def getWorkerIps: Future[List[String]] = async {
    await(registerAllComplete.future)
    val workerIps = registerRequests.asScala.toList.map(_.ip).sorted
    Check.workerIps(workerNum, workerIps)
    println(s"Master received all register requests, worker ips: $workerIps")
    workerIps
  }

  private def getRanges: Future[List[String]] = async {
    await(registerAllComplete.future)
    val keys = for {
      request <- registerRequests.asScala.toList
      sample <- request.samples
    } yield sample.key
    val portion: Int = (keys.length.toDouble / workerNum).ceil.toInt
    val ranges = keys.sorted.grouped(portion).map(_.head).toList
    Check.ranges(workerNum, ranges)
    println(s"Master received all register requests, ranges: $ranges")
    ranges
  }

  private def sendDistributeStart: Future[Unit] = async {
    val workerIps = await(this.workerIps)
    val ranges = await(this.ranges)
    val workerIpRangeMap: Map[String, String] = (workerIps zip ranges).toMap
    val channels = workerIps map { ip =>
      ManagedChannelBuilder.forAddress(ip, NetworkConfig.port).usePlaintext().build
    }
    val stubs: List[WorkerGrpc.WorkerStub] = channels map WorkerGrpc.stub
    val request: DistributeStartRequest = DistributeStartRequest(ranges = workerIpRangeMap)
    val responses: List[Future[DistributeStartResponse]] = stubs map (_.distributeStart(request))
    // No need to wait for responses
    println(s"Master sent distribute start request to all workers")
    ()
  }

  private def getDistributeCompleteWorkerIps: Future[List[String]] = async {
    await(distributeCompleteAllComplete.future)
    val workerIps = distributeCompleteRequests.asScala.toList.sorted
    Check.workerIps(workerNum, workerIps)
    assert(workerIps == await(this.workerIps))
    println(s"Master received all distribute complete requests, worker ips: $workerIps")
    workerIps
  }

  private def sendSortStart: Future[Unit] = async {
    val workerIps = await(distributeCompleteWorkerIps)
    val channels = workerIps map { ip =>
      ManagedChannelBuilder.forAddress(ip, NetworkConfig.port).usePlaintext().build
    }
    val stubs: List[WorkerGrpc.WorkerStub] = channels map WorkerGrpc.stub
    val request: SortStartRequest = SortStartRequest(success = true)
    val responses: List[Future[SortStartResponse]] = stubs map (_.sortStart(request))
    // no need to wait for responses
    println(s"Master sent sort start request to all workers")
    ()
  }
}
