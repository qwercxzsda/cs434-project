package com.blue.network

import java.net.InetAddress

object NetworkConfig {
  val masterIP = "2.2.2.142"
  val port = 30962
  val IP: String = InetAddress.getLocalHost.getHostAddress
}
