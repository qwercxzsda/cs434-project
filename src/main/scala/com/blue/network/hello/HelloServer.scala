package com.blue.network.hello

import com.blue.protos.hello._

import io.grpc.ServerBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

private class GreeterImpl extends GreeterGrpc.Greeter {
  override def sayHello(req: HelloRequest): Future[HelloReply] = {
    println(s"[server] greet request received $req")
    val reply = HelloReply(message = "Hello " + req.name)
    Future.successful(reply)
  }
}


object HelloServer extends App {
  val server = ServerBuilder.
    forPort(2242).
    addService(GreeterGrpc.bindService(new GreeterImpl, ExecutionContext.global)).
    build.
    start

  // block until shutdown
  if (server != null) {
    server.awaitTermination()
  }
}