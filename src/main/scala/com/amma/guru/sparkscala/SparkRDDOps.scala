package com.amma.guru.sparkscala

import java.io.File
import java.util
import scala.collection.mutable.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object SparkRDDOps {

  // set up from
  // https://www.atozlearner.com/distributed-computing/2018/12/13/setup-apache-spark-intellij/

  def main(args: Array[String]): Unit = {
    println("OM AMMA ..Please help")

    val path = getClass.getClassLoader.getResource("winutilPath.txt").getPath
    val file = new File(path)
    val wintPath = file.getParent
    System.setProperty("hadoop.home.dir", wintPath)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    /*Process a data set with two elements
    */
    println("***Process a data set with two elements***")
    val strSeq = Seq(("maths", 52), ("english", 75), ("science", 82), ("computer", 65), ("maths", 85))
    val data = spark.sparkContext.parallelize(strSeq)
    val abc = data.map(x => {
      ((x._1 + "AMMA"), (x._2) + 9999)
    })
    abc.foreach(println)

    /*
         Process a data set with mulitple
         and count the occurrence of distinct words
         in each row
         If file contains
          blue black green blue black
          blue black green blue black

          Out put should be :slight_smile:
          (blue , 2) ,(black , 2) ,(green,1)
          (blue , 2) ,(black , 2) ,(green,1)
              */
    println("*************\n" +
      "Process a data set with mulitple\n         " +
      "and count the occurrence of distinct words \n         " +
      "in each row" +
      " "
      "***")


    val strSeq1 = Seq(("black", "white","black","red"),
      ("AMMA", "KALI","AMMA","GURU"),
      ("AMMA", "AMMACHI","KALI","GURU"),
      ("AMMACH", "AMMACHI","KALI","RED"))

    val df1 = spark.sparkContext.parallelize(strSeq1)
    val op1 = df1.map(x => {
      // spark.sparkContext.parallelize
        val s = (Seq(x._1,x._2,x._3,x._4))
       // var map = new util.Map[String,Int]()
        var map1:Map[String,Int] =  Map()
       for(i <- s){
         if(map1.contains(i)){
           map1(i) = map1(i)+1
         }else{
           map1(i) = 1
         }

       }
      map1
       }
    )

    op1.foreach(println)



    println("groupByKey Test")
    val data1 = spark.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)))
    val group = data1.groupByKey()
    group.foreach(println)



    //reduce action test
    println("reduce Action Test")
    val exArray = Array("AMMA", "KALI", "MATHA", "GURU", "AMMACHI", "PONAMMA","AMMA","AMMAGURU", "GURU")
    val  wordCount = spark.sparkContext.parallelize(exArray)
    val wordcount1 =  wordCount.map(word => (word,1))
    println(s"wordcount1 RDD \n : ${wordcount1}")
    wordcount1.foreach(println)
    val wordcount2 = wordcount1.reduceByKey(_+_)
    wordcount2.foreach(println)

    spark.close()

  }

}
