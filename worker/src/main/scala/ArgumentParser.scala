package com.blue.worker

import com.blue.check.Check
import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec

object ArgumentParser {
  private val logger: Logger = Logger("ArgumentParser")

  def parse(args: List[String]): Map[String, List[String]] = {
    Check.weakAssert(logger)(args.nonEmpty, s"Too few arguments: ${args.length}")
    nextArg(Map(), "", args)
  }

  @tailrec
  private def nextArg(map: Map[String, List[String]], previousOption: String, list: List[String]): Map[String, List[String]] = {
    list match {
      case Nil => map
      case "-I" :: tail =>
        nextArg(map, "-I", tail)
      case "-O" :: tail =>
        nextArg(map, "-O", tail)
      case value :: tail =>
        val values = map.getOrElse(previousOption, List()) :+ value
        val newMap = map - previousOption + (previousOption -> values)
        nextArg(newMap, previousOption, tail)
    }
  }
}
