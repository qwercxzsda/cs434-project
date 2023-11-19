package com.blue.check

import com.blue.proto.sort.SortCompleteRequest
import com.typesafe.scalalogging.Logger

object Check {
  def workerIps(logger: Logger)(num: Int, ips: List[String]): Unit = {
    weakAssert(logger)(num == ips.length, s"worker num is $num, but worker ips length is ${ips.length}")

    // check strongly increasing(This also checks duplicate)
    (ips foldLeft "")((acc, ip) => {
      weakAssert(logger)(acc < ip, s"worker ips aren't strongly increasing: $acc, $ip")
      ip
    })
  }

  def ranges(logger: Logger)(num: Int, ranges: List[String]): Unit = {
    weakAssert(logger)(num == ranges.length, s"worker num is $num, but ranges length is ${ranges.length}")

    // check strongly increasing
    (ranges foldLeft "")((acc, range) => {
      weakAssert(logger)(acc < range, s"ranges aren't strongly increasing: $acc, $range")
      range
    })
  }

  def checkMasterResult(logger: Logger)(result: List[SortCompleteRequest]): Unit = {
    val sortedResult: List[SortCompleteRequest] = result sortBy (_.ip)
    (sortedResult foldLeft "")((acc: String, workerResult: SortCompleteRequest) => {
      val minKey: String = workerResult.begin.get.key
      val maxKey: String = workerResult.end.get.key
      logger.info(s"worker ${workerResult.ip} minKey: $minKey, maxKey: $maxKey")
      weakAssert(logger)(acc <= minKey, s"keys aren't increasing: $acc, $minKey")
      maxKey
    }
    )
  }

  // Similar to assert, but only logs error and does not throw exception
  def weakAssert(logger: Logger)(condition: Boolean, message: String): Unit = {
    if (!condition) logger.error(s"Assertion failed, $message")
  }

  def weakAssertEq(logger: Logger)(a: Any, b: Any, message: String): Unit = {
    weakAssert(logger)(a == b, s"$a != $b, $message")
  }
}
