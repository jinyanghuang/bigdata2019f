package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Q6 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    val date = args.date()

    if (args.text()) {
      val textFile = sc.textFile(args.input() + "/lineitem.tbl")
      val count = textFile
      .filter(line => line.split("\\|")(10).contains(date))
      .map(line => {
          val lineItem = line.split("\\|")
          val returnflag = lineItem(8)
          val lineStatus = lineItem(9)
          val quantity = lineItem(4).toInt
          val extendedPrice = lineItem(5).toDouble
          val discount = lineItem(6).toDouble
          val tax = lineItem(7).toDouble
          val discPrice = extendedPrice * (1 - discount)
          val charge = discPrice * (1 - tax)
          ((returnflag,lineStatus),(quantity, extendedPrice, discPrice, charge, discount, 1))
      }).reduceByKey((x,y) => (x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6))
      .collect()
      .foreach(p => {
          val count = p._2._6
          println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1/count, p._2._2/count, p._2._5/count, count)
      })
    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
  		val count = lineitemRDD
            .filter(line => line.getString(10).contains(date))
  			.map(line => {
                val returnflag = line.getString(8)
                val lineStatus = line.getString(9)
                val quantity = line.getInt(4)
                val extendedPrice = line.getDouble(5).
                val discount = line.getDouble(6).toDouble
                val tax = line.getDouble(7)
                val discPrice = extendedPrice * (1 - discount)
                val charge = discPrice * (1 - tax)
                ((returnflag,lineStatus),(quantity, extendedPrice, discPrice, charge, discount, 1)) 
              })
            .reduceByKey((x,y) => (x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6))
            .collect()
  			.foreach(p => {
                val count = p._2._6
                println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, p._2._1/count, p._2._2/count, p._2._5/count, count)
            })
        }
	}
}
