package com.blue.network

import java.net.InetAddress

object NetworkConfig {
  val ip: String = InetAddress.getLocalHost.getHostAddress
  val registerPort: Int = 30962
  val distributeStartPort: Int = 30963
  val distributePort: Int = 30964
  val distributeCompletePort: Int = 30965
  val sortStartPort: Int = 30966
  val sortCompletePort: Int = 30967
}
