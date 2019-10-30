package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession


object Q2 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    val date = args.date()

    if (args.text()) {
      val orderFile = sc.textFile(args.input() + "/orders.tbl")
      val order = orderFile.map(line => (line.split("\\|")(0).toInt,line.split("\\|")(6)))

      val textFile = sc.textFile(args.input() + "/lineitem.tbl")
      
      val result = textFile
      .map(line => (line.split("\\|")(0).toInt,line.split("\\|")(10)))
      .filter(_._2.contains(date))
      .cogroup(order)
      

      println("ANSWER=" + result)
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
  		val count = lineitemRDD
  			.map(line => line.getString(10))
  			.filter(_.contains(date))
  			.count

  		println("ANSWER=" + count)
    }
	}
}
