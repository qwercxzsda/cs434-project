package com.blue.network

import java.net.InetAddress

object NetworkConfig {
  val ip: String = InetAddress.getLocalHost.getHostAddress
  val masterPort: Int = 30962
  // Changed workerPort to var because of testing, do not change this in production
  var workerPort: Int = 30963
}
