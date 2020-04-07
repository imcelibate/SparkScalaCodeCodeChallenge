package com.amma.guru.sparkscala

import org.apache.spark.sql.SparkSession

object SparkRDDOps {

  // set up from
  // https://www.atozlearner.com/distributed-computing/2018/12/13/setup-apache-spark-intellij/

  def main(args: Array[String]): Unit = {
    println("OM AMMA ..Please help")
    val spark = SparkSession.builder().master("local").getOrCreate()
  }

}
