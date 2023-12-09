package com.blue.network

import java.net.InetAddress

object NetworkConfig {
  val ip: String = InetAddress.getLocalHost.getHostAddress
  val port: Int = 30962
}
