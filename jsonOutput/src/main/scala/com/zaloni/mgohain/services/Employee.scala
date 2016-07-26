package com.zaloni.mgohain.services

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mgohain on 7/22/2016.
  */
object Employee {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Incorrect number of arguments: " + args.length)
      System.out.println("Usage: " + this.getClass.getName + " input_file_path output_file_path")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("JSON output").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile(args(0))
    val words = lines.flatMap(line => line.split(","))
  }
}
