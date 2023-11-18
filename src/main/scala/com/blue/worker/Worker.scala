package com.blue.worker

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

object Worker extends App {
  private val masterIp: String = getMasterIp

  // TODO: change to proper argument parsing
  // TODO: should take multiple input directories
  private val inputDirectories: List[String] = List(args(2))
  private val outputDirectory: String = args(4)
  private val samples: List[Record] = getSamples

  sendRegister

  private val distributeStartComplete: Promise[Map[String, String]] = Promise()

  private val distributeComplete: Future[Unit] = sendDistribute

  sendDistributeComplete

  private val sortStartComplete: Promise[Unit] = Promise()

  private val sortComplete: Future[Unit] = sort

  private val workerComplete: Future[Unit] = sendSortComplete

  private val server = ServerBuilder.
    forPort(NetworkConfig.port).
    addService(WorkerGrpc.bindService(new WorkerImpl, ExecutionContext.global)).
    build.start

  // TODO: verify sort results, use logging
  /* All the code above executes asynchronously.
   * As as result, this part of code is reached immediately.
   */
  println(s"Worker server started at ${NetworkConfig.ip}:${NetworkConfig.port}")
  private val result: Unit = Await.result(workerComplete, Duration.Inf)

  private class WorkerImpl extends WorkerGrpc.Worker {
    override def distributeStart(request: DistributeStartRequest): Future[DistributeStartResponse] = {
      distributeStartComplete success request.ranges
      Future(DistributeStartResponse(success = true))
    }

    override def distribute(request: DistributeRequest): Future[DistributeResponse] = {
      val records = request.records
      // TODO: save records to file
      Future(DistributeResponse(success = true))
    }

    override def sortStart(request: SortStartRequest): Future[SortStartResponse] = {
      sortStartComplete success ()
      Future(SortStartResponse(success = true))
    }
  }

  private def getMasterIp: String = {
    assert(args(0).substring(args(0).indexOf(":")) == NetworkConfig.port.toString, s"input master port is not ${NetworkConfig.port}")
    args(0).substring(0, args(0).indexOf(":"))
  }

  private def getSamples: List[Record] = {
    // TODO: implement
    List()
  }

  private def sendRegister: Future[Unit] = async {
    val channel = ManagedChannelBuilder.forAddress(masterIp, NetworkConfig.port).usePlaintext().build
    val stub: MasterGrpc.MasterStub = MasterGrpc.stub(channel)
    val request: RegisterRequest = RegisterRequest(ip = NetworkConfig.ip, samples = samples)
    val response: Future[RegisterResponse] = stub.register(request)
    assert(await(response).ip == masterIp, s"sendRegisterResponse ip is not $masterIp")
    ()
  }

  private def sendDistribute: Future[Unit] = async {
    // TODO: implement
    await(distributeStartComplete.future)
    ()
  }

  private def sendDistributeComplete: Future[Unit] = async {
    // TODO: implement
    await(distributeComplete)
    ()
  }

  private def sort: Future[Unit] = async {
    // TODO: implement
    await(sortStartComplete.future)
    ()
  }

  private def sendSortComplete: Future[Unit] = async {
    // TODO: implement
    await(sortComplete)
    ()
  }
}
