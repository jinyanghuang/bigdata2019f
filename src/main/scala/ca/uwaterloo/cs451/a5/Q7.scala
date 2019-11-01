package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer


object Q7 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    val date = args.date()

    if (args.text()) {
      val customerFile = sc.textFile(args.input() + "/customer.tbl")
      val customer = customerFile.map(line => (line.split("\\|")(0).toInt,line.split("\\|")(1)))
      val customerBroadcast = sc.broadcast(customer.collectAsMap)

      val ordersFile = sc.textFile(args.input() + "/orders.tbl")
      val orders = ordersFile.map(line => (line.split("\\|")(0).toInt,line.split("\\|")(1).toInt,line.split("\\|")(4),line.split("\\|")(7)))
                    .map(line =>{
                        val orderKey = line._1
                        val customerKey = line._2
                        val orderDate = line._3
                        val shipPriority = line._4
                        val customerTable = customerBroadcast.value
                        (orderKey,(customerTable(customerKey),orderDate,shipPriority))
                    })
                    .filter(p => p._2._2 < date)

      val lineFile = sc.textFile(args.input() + "/lineitem.tbl")
      val result = lineFile
      .filter(line => line.split("\\|")(10) > date)
      .map(line => {
          val lineItem = line.split("\\|")
          val orderKey = lineItem(0).toInt
          val extendedPrice = lineItem(5).toDouble
          val discount = lineItem(6).toDouble
          val revenue = extendedPrice * (1-discount)
          (orderKey,revenue)
      })
      .reduceByKey(_ + _)
      .cogroup(orders)
      .filter( p => p._2._1.size != 0 && p._2._2.size != 0)
      .map(p =>{
          val customerName = p._2._2.head._1
          val orderKey = p._1
          val orderDate = p._2._2.head._2
          val shipPriority = p._2._2.head._3
          val revenue = p._2._1.foldLeft(0.0)((a, b) => a + b)
          (revenue, (customerName, orderKey, orderDate, shipPriority))
      })
      .sortByKey(false)
      .collect()
      .take(10)
      .map(p=>((p._2._1, p._2._2, p._2._3, p._2._4), p._1))
      .foreach(println) 

    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
  			
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
  	  val customer = customerRDD.map(line => (line.getInt(0),line.getString(1)))
      val customerBroadcast = sc.broadcast(customer.collectAsMap)

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD.map(line => (line.getInt(0),line.getInt(1),line.getString(4),line.getInt(7)))
                    .map(line =>{
                        val orderKey = line._1
                        val customerKey = line._2
                        val orderDate = line._3
                        val shipPriority = line._4
                        val customerTable = customerBroadcast.value
                        (orderKey,(customerTable(customerKey),orderDate,shipPriority))
                    })
                    .filter(p => p._2._2 < date)

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
  	  val result = lineitemRDD
  			.filter(line => line.getString(10) > date)
            .map(line => {
                val orderKey = line.getInt(0)
                val extendedPrice = line.getDouble(5)
                val discount = line.getDouble(6)
                val revenue = extendedPrice * (1-discount)
                (orderKey,revenue)
            })
            .reduceByKey(_ + _)
            .cogroup(orders)
            .filter( p => p._2._1.size != 0 && p._2._2.size != 0)
            .map(p =>{
                val customerName = p._2._2.head._1
                val orderKey = p._1
                val orderDate = p._2._2.head._2
                val shipPriority = p._2._2.head._3
                val revenue = p._2._1.foldLeft(0.0)((a, b) => a + b)
                (revenue, (customerName, orderKey, orderDate, shipPriority))
            })
            .sortByKey(false)
            .collect()
            .take(10)
            .map(p=>((p._2._1, p._2._2, p._2._3, p._2._4), p._1))
            .foreach(println) 
        }
	}
}
