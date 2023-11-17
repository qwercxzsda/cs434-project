package com.blue

import com.blue.proto.record._
import com.blue.proto.register._
import com.blue.proto.distribute_start._
import com.blue.proto.distribute_complete._
import com.blue.proto.sort_start._
import com.blue.proto.sort_complete._

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

object Master extends App {
  private val workerNum: Int = args(0).toInt

  private val registerRequests: ConcurrentLinkedQueue[RegisterRequest] = new ConcurrentLinkedQueue[RegisterRequest]()
  private val registerAllComplete: Promise[Unit] = Promise()
  private val registerServer = ServerBuilder.
    forPort(NetworkConfig.registerPort).
    addService(RegisterServiceGrpc.bindService(new RegisterImpl, ExecutionContext.global)).
    build.start
  private val workerIps: Future[List[String]] = getWorkerIps
  private val ranges: Future[List[String]] = getRanges

  sendDistributeStart

  private val distributeCompleteRequests: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
  private val distributeCompleteAllComplete: Promise[Unit] = Promise()
  private val distributeCompleteServer = ServerBuilder.
    forPort(NetworkConfig.distributeCompletePort).
    addService(DistributeCompleteServiceGrpc.bindService(new DistributeCompleteImpl, ExecutionContext.global)).
    build.start
  private val distributeCompleteWorkerIps: Future[List[String]] = getDistributeCompleteWorkerIps

  sendSortStart

  private val sortCompleteRequests: ConcurrentLinkedQueue[SortCompleteRequest] = new ConcurrentLinkedQueue[SortCompleteRequest]()
  private val sortCompleteAllComplete: Promise[Unit] = Promise()
  private val sortCompleteServer = ServerBuilder.
    forPort(NetworkConfig.sortCompletePort).
    addService(SortCompleteServiceGrpc.bindService(new SortCompleteImpl, ExecutionContext.global)).
    build.start
  // TODO: print result and wait the main thread until sortCompleteAllComplete.future is completed

  private class RegisterImpl extends RegisterServiceGrpc.RegisterService {
    override def register(request: RegisterRequest): Future[RegisterResponse] = {
      registerRequests add request
      if (registerRequests.size >= workerNum) {
        assert(registerRequests.size == workerNum)
        registerAllComplete trySuccess ()
      }
      Future(RegisterResponse(ip = NetworkConfig.ip, success = true))
    }
  }

  private class DistributeCompleteImpl extends DistributeCompleteServiceGrpc.DistributeCompleteService {
    override def distributeComplete(request: DistributeCompleteRequest): Future[DistributeCompleteResponse] = {
      distributeCompleteRequests add request.ip
      if (distributeCompleteRequests.size >= workerNum) {
        assert(distributeCompleteRequests.size == workerNum)
        distributeCompleteAllComplete trySuccess ()
      }
      Future(DistributeCompleteResponse(success = true))
    }
  }

  private class SortCompleteImpl extends SortCompleteServiceGrpc.SortCompleteService {
    override def sortComplete(request: SortCompleteRequest): Future[SortCompleteResponse] = {
      sortCompleteRequests add request
      if (sortCompleteRequests.size >= workerNum) {
        assert(sortCompleteRequests.size == workerNum)
        sortCompleteAllComplete trySuccess()
      }
      Future(SortCompleteResponse(success = true))
    }
  }

  private def getWorkerIps: Future[List[String]] = async {
    await(registerAllComplete.future)
    val workerIps = registerRequests.asScala.toList.map(_.ip).sorted
    Check.workerIps(workerNum, workerIps)
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
    ranges
  }

  private def sendDistributeStart: Future[Unit] = async {
    val workerIps = await(this.workerIps)
    val ranges = await(this.ranges)
    val workerIpRangeMap = (workerIps zip ranges).toMap
    // TODO: implement
    val channels = workerIps map { ip =>
      ManagedChannelBuilder.forAddress(ip, NetworkConfig.distributeStartPort).usePlaintext().build
    }
    val stubs: List[DistributeStartServiceGrpc.DistributeStartServiceStub] =
      channels map DistributeStartServiceGrpc.stub
    val request: DistributeStartRequest = DistributeStartRequest(ranges = workerIpRangeMap)
    val responses: List[Future[DistributeStartResponse]] = stubs map (_.distributeStart(request))
    // No need to wait for responses
    ()
  }

  private def getDistributeCompleteWorkerIps: Future[List[String]] = async {
    await(distributeCompleteAllComplete.future)
    val workerIps = distributeCompleteRequests.asScala.toList.sorted
    Check.workerIps(workerNum, workerIps)
    assert(workerIps == await(this.workerIps), "getDistributeCompleteWorkerIps is not equal to workerIps")
    workerIps
  }

  private def sendSortStart: Future[Unit] = async {
    // TODO: implement
    ()
  }
}
