package com.blue.network.hello

import com.blue.protos.hello._

import io.grpc.ManagedChannelBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object HelloClient extends App {
  // build gRPC channel
  val channel =
    ManagedChannelBuilder
      .forAddress("141.223.16.227", 2242)
      .usePlaintext()
      .build

  // blocking and async stubs
  val blockingStub = GreeterGrpc.blockingStub(channel)
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
    println(s"[client] greet request $request")

    val reply: Future[HelloReply] = stub.sayHello(request)

    reply.onComplete {
      case Success(reply) =>
        println(s"[client] greet reply $reply")
      case Failure(e) =>
        e.printStackTrace()
    }
  }

  //  val user = args.headOption.getOrElse("world")
  val user = "client1"
  greet(user)

  // wait main thread till futures/streams to complete
  // otherwise main thread will exit before printing the results
  while (true) {}
}
