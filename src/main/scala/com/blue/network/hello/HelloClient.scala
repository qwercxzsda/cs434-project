package com.blue.network.hello

import com.blue.protos.hello._
import com.blue.network.NetworkConfig

import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object HelloClient extends App {
  // build gRPC channel
  val channel =
    ManagedChannelBuilder
      .forAddress(NetworkConfig.masterIP, NetworkConfig.port)
      .usePlaintext()
      .build

  // blocking and async stubs
  //  val blockingStub = GreeterGrpc.blockingStub(channel)
  val stub = GreeterGrpc.stub(channel)

  //  def greetBlocking(name: String): Unit = {
  //    val request = HelloRequest(name = name)
  //    println(s"[unary] blocking greet request $request")
  //
  //    val reply: HelloReply = blockingStub.sayHello(request)
  //    println(s"[unary] blocking greet reply $reply")
  //  }

  def greet(name: String): Unit = {
    val request = HelloRequest(name = name)
    println(s"client: send greet request $request")

    val reply: Future[HelloReply] = stub.sayHello(request)

    reply.onComplete {
      case Success(reply) =>
        println(s"client: receive greet reply $reply")
      case Failure(e) =>
        e.printStackTrace()
    }
  }

  println(s"client: started on ip ${NetworkConfig.IP}")
  //  val user = args.headOption.getOrElse("world")
  greet(NetworkConfig.IP)

  // wait main thread till futures/streams to complete
  // otherwise main thread will exit before printing the results
  while (true) {}
}
