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

  private def getWorkerIps: Future[List[String]] = async {
    await(registerAllComplete.future)
    val workerIps = registerRequests.asScala.toList.map(_.ip)
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
}
