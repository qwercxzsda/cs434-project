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
import com.blue.bytestring_ordering.ByteStringOrdering._
import scala.math.Ordered.orderingToOrdered
import io.grpc.{Server, ServerBuilder}
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import scala.io.BufferedSource
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.util.{Failure, Success}
import scala.concurrent.Await
import scala.concurrent.duration._

object Worker extends App {
  private val logger: Logger = Logger("Worker")
  private val arguments: Map[String, List[String]] = ArgumentParser.parse(args.toList)
  Check.weakAssert(logger)((arguments("").length == 1) && (arguments("-O").length == 1), s"Too many arguments: $arguments")
  private val masterIp: String = getMasterIp(arguments("").head)

  private val inputDirectories: List[String] = arguments("-I")
  private val outputDirectory: String = arguments("-O").head
  private val recordFileManipulator: RecordFileManipulator = new RecordFileManipulator(inputDirectories, outputDirectory)

  private val samples: Future[List[Record]] = getSamples
  sendRegister

  private val distributeStartComplete: Promise[Map[String, ByteString]] = Promise()
  private val distributeComplete: Future[Unit] = sendDistribute
  sendDistributeComplete

  private val sortStartComplete: Promise[Unit] = Promise()
  private val sortComplete: Future[Unit] = sort
  private val workerComplete: Future[Unit] = sendSortComplete

  private val server = ServerBuilder.
    forPort(NetworkConfig.port).
    addService(WorkerGrpc.bindService(new WorkerImpl, ExecutionContext.global)).
    asInstanceOf[ServerBuilder[_]].
    build.start

  /* All the code above executes asynchronously.
   * As as result, this part of code is reached immediately.
   */
  logger.info(s"Server started at ${NetworkConfig.ip}:${NetworkConfig.port}")
  blocking {
    Await.result(workerComplete, Duration.Inf)
  }
  logger.info(s"Finished sorting, workerComplete")

  private class WorkerImpl extends WorkerGrpc.Worker {
    override def distributeStart(request: DistributeStartRequest): Future[DistributeStartResponse] = {
      logger.info(s"Received DistributeStartRequest with ranges: ${request.ranges}")
      distributeStartComplete success request.ranges
      Future(DistributeStartResponse())
    }

    override def distribute(request: DistributeRequest): Future[DistributeResponse] = {
      val records = request.records
      logger.info(s"Received DistributeRequest from ${request.ip} with ${records.size} records")
      recordFileManipulator saveDistributedRecords records
      Future(DistributeResponse())
    }

    override def sortStart(request: SortStartRequest): Future[SortStartResponse] = {
      logger.info(s"Received SortStartRequest")
      sortStartComplete success ()
      Future(SortStartResponse())
    }
  }

  private def getMasterIp(ipAndPort: String): String = {
    val indexDiv: Int = ipAndPort.indexOf(":")
    Check.weakAssert(logger)(indexDiv != -1, s"ipAndPort doesn't contain ':', ipAndPort: $ipAndPort")
    val port: String = ipAndPort.substring(indexDiv).drop(1)
    Check.weakAssertEq(logger)(port, NetworkConfig.port.toString, s"port(from argument) is not equal to NetworkConfig.port")
    ipAndPort.substring(0, indexDiv)
  }

  private def getSamples: Future[List[Record]] = async {
    logger.info(s"Sampling started")
    recordFileManipulator.getSamples
  }

  // Send Register request to master
  private def sendRegister: Future[Unit] = async {
    val samples = await(this.samples)
    val channel = ManagedChannelBuilder.forAddress(masterIp, NetworkConfig.port).
      usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
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
    val ranges: Map[String, ByteString] = await(distributeStartComplete.future)
    val workerIps: List[String] = ranges.keys.toList
    val rangeBegins: List[ByteString] = ranges.values.toList
    val (recordsToDistribute: Iterator[Record], toClose: BufferedSource) = recordFileManipulator.getRecordsToDistribute
    try {
      logger.info(s"Sending DistributeRequest for all samples to designated workers(Distribution started)")
      val channels = workerIps map { ip =>
        ManagedChannelBuilder.forAddress(ip, NetworkConfig.port).
          usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
      }
      val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub

      def distributeOneRecord(record: Record): Unit = {
        val key = record.key
        // send to the last worker whose rangeBegin is greater than or equal to the key
        val blockingStubIdx: Int = rangeBegins lastIndexWhere (rangeBegin => key >= rangeBegin)
        // As rangeBegins are calculated from samples, there might be a record whose key is smaller than all rangeBegins
        // In this case, send the record to the first worker
        val blockingStub: WorkerGrpc.WorkerBlockingStub = blockingStubs(if (blockingStubIdx == -1) 0 else blockingStubIdx)
        // TODO: send blocks of records for efficiency
        val request: DistributeRequest = DistributeRequest(ip = NetworkConfig.ip, records = Seq(record))
        val response: DistributeResponse = blockingStub.distribute(request)
      }

      blocking {
        recordsToDistribute foreach distributeOneRecord
      }
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
    val channel = ManagedChannelBuilder.forAddress(masterIp, NetworkConfig.port).
      usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
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
    val channel = ManagedChannelBuilder.forAddress(masterIp, NetworkConfig.port).
      usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
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
