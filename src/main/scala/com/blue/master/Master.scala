package com.blue.master

import com.blue.proto.record._
import com.blue.proto.register._
import com.blue.proto.distribute._
import com.blue.proto.sort._
import com.blue.proto.master._
import com.blue.proto.worker._
import com.blue.network.NetworkConfig
import com.blue.check.Check
import com.blue.proto.address.Address
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
  private val workerIps: Future[List[Address]] = getWorkerIps
  private val ranges: Future[List[ByteString]] = getRanges
  private val workerChannels: Future[List[ManagedChannel]] = getWorkerChannels
  sendDistributeStart

  private val distributeCompleteRequests: ConcurrentLinkedQueue[Address] = new ConcurrentLinkedQueue[Address]()
  private val distributeCompleteAllComplete: Promise[Unit] = Promise()
  private val distributeCompleteWorkerIps: Future[List[Address]] = getDistributeCompleteWorkerIps
  sendSortStart

  private val sortCompleteRequests: ConcurrentLinkedQueue[SortCompleteRequest] = new ConcurrentLinkedQueue[SortCompleteRequest]()
  private val sortCompleteAllComplete: Promise[Unit] = Promise()

  private val server = ServerBuilder.
    forPort(NetworkConfig.masterPort).
    addService(MasterGrpc.bindService(new MasterImpl, ExecutionContext.global)).
    asInstanceOf[ServerBuilder[_]].
    build.start

  /* All the code above executes asynchronously.
   * As as result, this part of code is reached immediately.
   */
  logger.info(s"Server started at ${NetworkConfig.ip}:${NetworkConfig.masterPort}")
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
      logger.info(s"Received register request from ${request.address}")
      registerRequests add request
      if (registerRequests.size >= workerNum) {
        Check.weakAssertEq(logger)(registerRequests.size, workerNum, "registerRequests.size is not equal to workerNum")
        registerAllComplete trySuccess ()
      }
      Future(RegisterResponse(address = Some(Address(ip = NetworkConfig.ip, port = NetworkConfig.masterPort))))
    }

    override def distributeComplete(request: DistributeCompleteRequest): Future[DistributeCompleteResponse] = {
      logger.info(s"Received distribute complete request from ${request.address}")
      distributeCompleteRequests add request.address.get
      if (distributeCompleteRequests.size >= workerNum) {
        Check.weakAssertEq(logger)(distributeCompleteRequests.size, workerNum, s"distributeCompleteRequests.size is not equal to workerNum")
        distributeCompleteAllComplete trySuccess ()
      }
      Future(DistributeCompleteResponse())
    }

    override def sortComplete(request: SortCompleteRequest): Future[SortCompleteResponse] = {
      logger.info(s"Received sort complete request from ${request.address}")
      sortCompleteRequests add request
      if (sortCompleteRequests.size >= workerNum) {
        Check.weakAssertEq(logger)(sortCompleteRequests.size, workerNum, s"sortCompleteRequests.size is not equal to workerNum")
        sortCompleteAllComplete trySuccess ()
      }
      Future(SortCompleteResponse())
    }
  }

  private def getWorkerIps: Future[List[Address]] = async {
    await(registerAllComplete.future)
    val workerIps = registerRequests.asScala.toList.map(_.address.get).sortBy(address => (address.ip, address.port))
    Check.workerIps(logger)(workerNum, workerIps.map(address => (address.ip, address.port)))
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
    val workerIpRangeMap: List[Range] = (workerIps zip ranges).map(ipRange => Range(address = Some(ipRange._1), beginKey = ipRange._2))
    val channels = await(workerChannels)
    val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub
    val request: DistributeStartRequest = DistributeStartRequest(ranges = workerIpRangeMap)
    val responses: List[DistributeStartResponse] = blockingStubs map (_.distributeStart(request))
    // No need to wait for responses
    logger.info(s"Sent distribute start request to all workers")
    ()
  }

  private def getDistributeCompleteWorkerIps: Future[List[Address]] = async {
    await(distributeCompleteAllComplete.future)
    val workerIps = distributeCompleteRequests.asScala.toList
    Check.workerIps(logger)(workerNum, workerIps.map(address => (address.ip, address.port)))
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
    val workerIps: List[Address] = await(this.workerIps)
    val workerChannels: List[ManagedChannel] = workerIps map { ip =>
      ManagedChannelBuilder.forAddress(ip.ip, ip.port).
        usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    }
    logger.info(s"opened channels to all workers")
    workerChannels
  }
}
