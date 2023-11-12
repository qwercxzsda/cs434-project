package com.blue.network.hello

import com.blue.protos.hello._
import com.blue.network.NetworkConfig

import io.grpc.{Server, ServerBuilder}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

private class GreeterImpl extends GreeterGrpc.Greeter {
  override def sayHello(req: HelloRequest): Future[HelloReply] = {
    println(s"server: receive greet request $req")
    val reply = HelloReply(message = "Hello " + req.name)
    Future.successful(reply)
  }
}


object HelloServer extends App {
  val server = ServerBuilder.
    forPort(NetworkConfig.port).
    addService(GreeterGrpc.bindService(new GreeterImpl, ExecutionContext.global)).
    build.
    start

  println(s"server: started on ip ${NetworkConfig.IP}, port ${NetworkConfig.port}")
  // block until shutdown
  if (server != null) {
    server.awaitTermination()
  }
}