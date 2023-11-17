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
import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.util.{Failure, Success}

object Master extends App {
  private val workerNum: Int = args(0).toInt

  private val registerRequests: ConcurrentLinkedQueue[RegisterRequest] = new ConcurrentLinkedQueue[RegisterRequest]()
  private val registerAllComplete: Promise[Unit] = Promise()
  private val workerIps: Future[List[String]] = getWorkerIps
  private val ranges: Future[List[String]] = getRanges

  sendRanges

  private val distributeCompleteRequests: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
  private val distributeCompleteAllComplete: Promise[Unit] = Promise()
  private val distributeCompleteWorkerIps: Future[List[String]] = getDistributeCompleteWorkerIps

  private class RegisterImpl extends RegisterServiceGrpc.RegisterService {
    override def register(request: RegisterRequest): Future[RegisterResponse] = {
      registerRequests add request
      if (registerRequests.size >= workerNum) {
        assert(registerRequests.size == workerNum)
        registerAllComplete success()
      }
      Future(RegisterResponse(ip = NetworkConfig.ip, success = true))
    }
  }

  private class DistributeCompleteImpl extends DistributeCompleteServiceGrpc.DistributeCompleteService {
    override def distributeComplete(request: DistributeCompleteRequest): Future[DistributeCompleteResponse] = {
      distributeCompleteRequests add request.ip
      if (distributeCompleteRequests.size >= workerNum) {
        assert(distributeCompleteRequests.size == workerNum)
        distributeCompleteAllComplete success()
      }
      Future(DistributeCompleteResponse(success = true))
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

  private def sendRanges: Future[Unit] = async {
    val workerIpsVal = await(workerIps)
    val rangesVal = await(ranges)
    val workerIpRangeMap = (workerIpsVal zip rangesVal).toMap
    // TODO: implement
    ()
  }

  private def getDistributeCompleteWorkerIps: Future[List[String]] = async {
    await(distributeCompleteAllComplete.future)
    val workerIps = distributeCompleteRequests.asScala.toList.sorted
    Check.workerIps(workerNum, workerIps)
    assert(workerIps == await(this.workerIps), "getDistributeCompleteWorkerIps is not equal to workerIps")
    workerIps
  }
}
