package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer


object Q4 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    val date = args.date()

    if (args.text()) {
      val nationFile = sc.textFile(args.input() + "/nation.tbl")
      val nation = nationFile.map(line => (line.split("\\|")(0).toInt,line.split("\\|")(1)))
      val nationBroadcast = sc.broadcast(nation.collectAsMap)

      val customerFile = sc.textFile(args.input() + "/customer.tbl")
      val customer = customerFile.map(line => (line.split("\\|")(0).toInt,line.split("\\|")(3).toInt))
                    .map(line =>{
                        val customerKey = line._1
                        val nationKey = line._2
                        val nationTable = nationBroadcast.value
                        (customerKey,(nationKey,nationTable(nationKey)))
                    })
      val customerBroadcast = sc.broadcast(customer.collectAsMap)

      val ordersFile = sc.textFile(args.input() + "/orders.tbl")
      val orders = ordersFile.map(line => (line.split("\\|")(0).toInt,line.split("\\|")(1).toInt))
                    .map(line =>{
                        val orderKey = line._1
                        val customerKey = line._2
                        val customerTable = customerBroadcast.value
                        (orderKey,(customerTable(customerKey)))
                    })
      
      
      val lineFile = sc.textFile(args.input() + "/lineitem.tbl")
      val result = lineFile
      .map(line => (line.split("\\|")(0).toInt,line.split("\\|")(10)))
      .filter(_._2.contains(date))
      .cogroup(orders)
      .filter(_._2._1.size != 0)
      .flatMap(line=>{
          val nationGroup = new ListBuffer[(Int, String)]()
          nationGroup += (line._2._2.head)
          nationGroup.toList
      }).map(pair => (pair,1))
      .reduceByKey(_ + _)
      .map(p => (p._1._1,(p._1._2,p._2)))
      .sortByKey()
      .map(p => ((p._1,p._2._1),p._2._2))
      .foreach(println) 

    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd
  	  val nation = nationRDD
  			.map(line => (line.getInt(0),line.getString(1)))
      val nationBroadcast = sc.broadcast(nation.collectAsMap)
  			
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
  	  val customer = customerRDD
  			.map(line => (line.getInt(0),line.getInt(3)))
            .map(line =>{
                    val customerKey = line._1
                    val nationKey = line._2
                    val nationTable = nationBroadcast.value
                    (customerKey,(nationKey,nationTable(nationKey)))
                })
      val customerBroadcast = sc.broadcast(customer.collectAsMap)

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD.map(line => (line.getInt(0),line.getInt(1)))
                    .map(line =>{
                        val orderKey = line._1
                        val customerKey = line._2
                        val customerTable = customerBroadcast.value
                        (orderKey,(customerTable(customerKey)))
                    })

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
  	  val result = lineitemRDD
  			.map(line => (line.getInt(0),line.getString(10)))
  			.filter(_._2.contains(date))
            .cogroup(orders)
            .filter(_._2._1.size != 0)
            .flatMap(line=>{
                val nationGroup = new ListBuffer[(Int, String)]()
                nationGroup += (line._2._2.head)
                nationGroup.toList
            }).map(pair => (pair,1))
            .reduceByKey(_ + _)
            .map(p => (p._1._1,(p._1._2,p._2)))
            .sortByKey()
            .map(p => ((p._1,p._2._1),p._2._2))
            .foreach(println) 
        }
	}
}
