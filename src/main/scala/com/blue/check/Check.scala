package com.blue.check

object Check {
  def workerIps(num: Int, ips: List[String]): Unit = {
    assert(num == ips.length, s"worker num is $num, but worker ips length is ${ips.length}")

    // check strongly increasing(This also checks duplicate)
    (ips foldLeft "")((acc, ip) => {
      assert(acc < ip, s"worker ips is not strongly increasing: $acc, $ip")
      ip
    })
  }

  def ranges(num: Int, ranges: List[String]): Unit = {
    assert(num == ranges.length, s"worker num is $num, but ranges length is ${ranges.length}")

    // check strongly increasing
    (ranges foldLeft "")((acc, range) => {
      assert(acc < range, s"ranges is not strongly increasing: $acc, $range")
      range
    })
  }
}
