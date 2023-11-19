package com.blue.worker

import com.blue.proto.record._
import com.blue.proto.register._
import com.blue.proto.distribute._
import com.blue.proto.sort._
import com.blue.proto.master._
import com.blue.proto.worker._

import com.blue.network.NetworkConfig
import com.blue.check.Check
import com.blue.record_file_manipulator.RecordFileManipulator

import com.google.protobuf.ByteString
import io.grpc.{Server, ServerBuilder}
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import scala.io.BufferedSource
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.util.{Failure, Success}
import scala.concurrent.Await
import scala.concurrent.duration._

object Worker extends App {
  private val logger: Logger = Logger("Worker")
  private val masterIp: String = getMasterIp

  // TODO: change to proper argument parsing
  // TODO: should take multiple input directories
  private val inputDirectories: List[String] = List(args(2))
  private val outputDirectory: String = args(4)
  private val recordFileManipulator: RecordFileManipulator = new RecordFileManipulator(inputDirectories, outputDirectory)
  private val samples: Future[List[Record]] = getSamples

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

  /* All the code above executes asynchronously.
   * As as result, this part of code is reached immediately.
   */
  logger.info(s"Server started at ${NetworkConfig.ip}:${NetworkConfig.port}")
  Await.result(workerComplete, Duration.Inf)
  logger.info(s"Finished sorting, workerComplete")

  private class WorkerImpl extends WorkerGrpc.Worker {
    override def distributeStart(request: DistributeStartRequest): Future[DistributeStartResponse] = {
      logger.info(s"Received DistributeStartRequest with ranges: ${request.ranges}")
      distributeStartComplete success request.ranges
      Future(DistributeStartResponse())
    }

    override def distribute(request: DistributeRequest): Future[DistributeResponse] = {
      val records = request.records
      logger.info(s"Received DistributeRequest with ${records.size} records")
      recordFileManipulator saveDistributedRecords records
      Future(DistributeResponse())
    }

    override def sortStart(request: SortStartRequest): Future[SortStartResponse] = {
      logger.info(s"Received SortStartRequest")
      sortStartComplete success ()
      Future(SortStartResponse())
    }
  }

  private def getMasterIp: String = {
    val port: String = args(0).substring(args(0).indexOf(":")).drop(1)
    Check.weakAssertEq(logger)(port, NetworkConfig.port.toString, s"port(from argument) is not equal to NetworkConfig.port")
    args(0).substring(0, args(0).indexOf(":"))
  }

  private def getSamples: Future[List[Record]] = async {
    logger.info(s"Sampling started")
    recordFileManipulator.getSamples
  }

  // Send Register request to master
  private def sendRegister: Future[Unit] = async {
    val samples = await(this.samples)
    val channel = ManagedChannelBuilder.forAddress(masterIp, NetworkConfig.port).usePlaintext().build
    val stub: MasterGrpc.MasterStub = MasterGrpc.stub(channel)
    val request: RegisterRequest = RegisterRequest(ip = NetworkConfig.ip, samples = samples)
    val response: Future[RegisterResponse] = stub.register(request)
    Check.weakAssertEq(logger)(await(response).ip, masterIp, "await(response).ip is not equal to masterIp")
    logger.info(s"Sent RegisterRequest to master at $masterIp:${NetworkConfig.port}(Wait for response)")
    ()
  }

  // Send Distribute(i.e., send block of records) request to designated worker
  // This distributes(shuffles) records among workers
  private def sendDistribute: Future[Unit] = async {
    val ranges: Map[String, String] = await(distributeStartComplete.future)
    val workerIps: List[String] = ranges.keys.toList
    val rangeBegins: List[String] = ranges.values.toList
    val (recordsToDistribute: Iterator[Record], toClose: BufferedSource) = recordFileManipulator.getRecordsToDistribute
    try {
      val channels = workerIps map { ip =>
        ManagedChannelBuilder.forAddress(ip, NetworkConfig.port).usePlaintext().build
      }
      val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub
      val rangeBegin_blockingStubs: List[(String, WorkerGrpc.WorkerBlockingStub)] = rangeBegins zip blockingStubs

      def distributeOneRecord(record: Record): Unit = {
        val key = record.key
        // send to the last worker whose rangeBegin is greater than or equal to the key
        val blockingStub = (rangeBegin_blockingStubs findLast (rangeBegin_stub => key >= rangeBegin_stub._1)).get._2
        // TODO: send blocks of records for efficiency
        val request: DistributeRequest = DistributeRequest(records = Seq(record))
        val response: DistributeResponse = blockingStub.distribute(request)
      }

      recordsToDistribute foreach distributeOneRecord
    } finally {
      recordFileManipulator.closeRecordsToDistribute(toClose)
    }
    // Must wait for response
    // If not, the worker will start sorting before all records are distributed
    // Thus, we use blockingStub to send DistributeRequest
    logger.info(s"Sent DistributeRequest for all samples to designated workers(Wait for response)")
    ()
  }

  // Send DistributeComplete request to master
  // This notifies master that this worker has finished distributing all records
  private def sendDistributeComplete: Future[Unit] = async {
    await(distributeComplete)
    val channel = ManagedChannelBuilder.forAddress(masterIp, NetworkConfig.port).usePlaintext().build
    val stub: MasterGrpc.MasterStub = MasterGrpc.stub(channel)
    val request: DistributeCompleteRequest = DistributeCompleteRequest(ip = NetworkConfig.ip)
    val response: Future[DistributeCompleteResponse] = stub.distributeComplete(request)
    // No need to wait for response
    logger.info(s"Sent DistributeCompleteRequest to master(Didn't wait for response)")
    ()
  }

  // Sort the records that is distributed(shuffled) to this worker
  // Start the sort process after sortStartComplete.future is completed
  private def sort: Future[Unit] = async {
    await(sortStartComplete.future)
    logger.info(s"Sorting distributed records started")
    recordFileManipulator.sortDistributedRecords()
  }

  // Send SortComplete request to master
  // This notifies master that this worker has finished sorting all records that is distributed(shuffled)
  private def sendSortComplete: Future[Unit] = async {
    await(sortComplete)
    val sortResult: (Record, Record) = recordFileManipulator.getSortResult
    val channel = ManagedChannelBuilder.forAddress(masterIp, NetworkConfig.port).usePlaintext().build
    val stub: MasterGrpc.MasterStub = MasterGrpc.stub(channel)
    val request: SortCompleteRequest =
      SortCompleteRequest(ip = NetworkConfig.ip, begin = Option(sortResult._1), end = Option(sortResult._2))
    val response: Future[SortCompleteResponse] = stub.sortComplete(request)

    logger.info(s"Sort complete, minKey: ${sortResult._1.key}, maxKey: ${sortResult._2.key}")
    // Must wait for response
    await(response)
    logger.info(s"Sent SortCompleteRequest to master(Wait for response)")
    ()
  }
}
