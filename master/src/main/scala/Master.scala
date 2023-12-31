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
import com.blue.bytestring_ordering.ByteStringOrdering._
import scala.math.Ordered.orderingToOrdered
import io.grpc.{Server, ServerBuilder}
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import scala.math.Ordering.comparatorToOrdering
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.util.{Failure, Success}
import scala.concurrent.Await
import scala.concurrent.duration._

object Master extends App {
  private val logger: Logger = Logger("Master")
  private val workerNum: Int = args(0).toInt

  private val registerRequests: ConcurrentLinkedQueue[RegisterRequest] = new ConcurrentLinkedQueue[RegisterRequest]()
  private val registerAllComplete: Promise[Unit] = Promise()
  private val workerIps: Future[List[String]] = getWorkerIps
  private val ranges: Future[List[ByteString]] = getRanges
  private val workerChannels: Future[List[ManagedChannel]] = getWorkerChannels
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
    asInstanceOf[ServerBuilder[_]].
    build.start

  /* All the code above executes asynchronously.
   * As as result, this part of code is reached immediately.
   */
  logger.info(s"Server started at ${NetworkConfig.ip}:${NetworkConfig.port}")
  blocking {
    Await.result(sortCompleteAllComplete.future, Duration.Inf)
  }
  logger.info(s"All the workers finished sorting, MasterComplete")
  blocking {
    Await.result(workerChannels, Duration.Inf).foreach(_.shutdown())
  }
  server.shutdown()
  private val result: List[SortCompleteRequest] = sortCompleteRequests.asScala.toList
  Check.weakAssertEq(logger)(result.length, workerNum, "result.length is not equal to workerNum")
  Check.masterResult(logger)(result)

  private class MasterImpl extends MasterGrpc.Master {
    override def register(request: RegisterRequest): Future[RegisterResponse] = {
      logger.info(s"Received register request from ${request.ip}")
      registerRequests add request
      if (registerRequests.size >= workerNum) {
        Check.weakAssertEq(logger)(registerRequests.size, workerNum, "registerRequests.size is not equal to workerNum")
        registerAllComplete trySuccess ()
      }
      Future(RegisterResponse(ip = NetworkConfig.ip))
    }

    override def distributeComplete(request: DistributeCompleteRequest): Future[DistributeCompleteResponse] = {
      logger.info(s"Received distribute complete request from ${request.ip}")
      distributeCompleteRequests add request.ip
      if (distributeCompleteRequests.size >= workerNum) {
        Check.weakAssertEq(logger)(distributeCompleteRequests.size, workerNum, s"distributeCompleteRequests.size is not equal to workerNum")
        distributeCompleteAllComplete trySuccess ()
      }
      Future(DistributeCompleteResponse())
    }

    override def sortComplete(request: SortCompleteRequest): Future[SortCompleteResponse] = {
      logger.info(s"Received sort complete request from ${request.ip}")
      sortCompleteRequests add request
      if (sortCompleteRequests.size >= workerNum) {
        Check.weakAssertEq(logger)(sortCompleteRequests.size, workerNum, s"sortCompleteRequests.size is not equal to workerNum")
        sortCompleteAllComplete trySuccess ()
      }
      Future(SortCompleteResponse())
    }
  }

  private def getWorkerIps: Future[List[String]] = async {
    await(registerAllComplete.future)
    val workerIps = registerRequests.asScala.toList.map(_.ip).sorted
    Check.workerIps(logger)(workerNum, workerIps)
    logger.info(s"Received all register requests, worker ips: $workerIps")
    workerIps
  }

  private def getRanges: Future[List[ByteString]] = async {
    await(registerAllComplete.future)
    val keys = for {
      request <- registerRequests.asScala.toList
      sample <- request.samples
    } yield sample.key
    val portion: Int = (keys.length.toDouble / workerNum).ceil.toInt
    val ranges = keys.sorted.grouped(portion).map(_.head).toList
    Check.ranges(logger)(workerNum, ranges)
    logger.info(s"Received all register requests, ranges: $ranges")
    ranges
  }

  private def sendDistributeStart: Future[Unit] = async {
    val workerIps = await(this.workerIps)
    val ranges = await(this.ranges)
    val workerIpRangeMap: Map[String, ByteString] = (workerIps zip ranges).toMap
    val channels = await(workerChannels)
    val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub
    val request: DistributeStartRequest = DistributeStartRequest(ranges = workerIpRangeMap)
    val responses: List[DistributeStartResponse] = blockingStubs map (_.distributeStart(request))
    // No need to wait for responses
    logger.info(s"Sent distribute start request to all workers")
    ()
  }

  private def getDistributeCompleteWorkerIps: Future[List[String]] = async {
    await(distributeCompleteAllComplete.future)
    val workerIps = distributeCompleteRequests.asScala.toList.sorted
    Check.workerIps(logger)(workerNum, workerIps)
    Check.weakAssertEq(logger)(workerIps, await(this.workerIps), "getDistributeCompleteWorkerIps is not equal to workerIps")
    logger.info(s"Received all distribute complete requests, worker ips: $workerIps")
    workerIps
  }

  private def sendSortStart: Future[Unit] = async {
    val workerIps = await(distributeCompleteWorkerIps)
    val channels = await(workerChannels)
    val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub
    val request: SortStartRequest = SortStartRequest()
    val responses: List[SortStartResponse] = blockingStubs map (_.sortStart(request))
    // no need to wait for responses
    logger.info(s"Sent sort start request to all workers")
    ()
  }

  private def getWorkerChannels: Future[List[ManagedChannel]] = async {
    val workerIps: List[String] = await(this.workerIps)
    val workerChannels: List[ManagedChannel] = workerIps map { ip =>
      ManagedChannelBuilder.forAddress(ip, NetworkConfig.port).
        usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    }
    logger.info(s"opened channels to all workers")
    workerChannels
  }
}
