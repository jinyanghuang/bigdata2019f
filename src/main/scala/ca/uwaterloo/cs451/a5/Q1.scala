package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "use text file", required = false)
  val parquet = opt[Boolean](descr = "use parquet file", required = false)
  verify()
}

object Q1 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val textFile = sc.textFile(args.input() + "/lineitem.tbl")
      val count = textFile
      .map(line => line.split("\\|")(10))
      .filter(_.contains(date))
      .count

      println("ANSWER=" + count)
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
