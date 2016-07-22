package com.zaloni.mgohain.services

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * This object will monitor a directory for new files and calculates the word counts
  * Created by mgohain on 7/22/2016.
  */
object WordCount {
  def main(args : Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: " + WordCount.getClass.getSimpleName + " input_directory_name output_directory_name")
      System.err.println("Give the directory path")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("Word Count").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    //Map phase
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(line => line.split(","))
    val wordMap = words.map(word => (word,1))
    //Reduce phase
    val counts = wordMap.reduceByKey(_+_)
    counts.print()
    counts.saveAsTextFiles(args(1))
    ssc.start()
    ssc.awaitTermination()
  }
}
