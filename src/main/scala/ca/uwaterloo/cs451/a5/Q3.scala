package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession


object Q3 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    val date = args.date()

    if (args.text()) {
      val partFile = sc.textFile(args.input() + "/part.tbl")
      val part = partFile.map(line => (line.split("\\|")(0).toInt,line.split("\\|")(1)))
      val partBroadcast = sc.broadcast(part.collectAsMap)

      val supplierFile = sc.textFile(args.input() + "/supplier.tbl")
      val supplier = supplierFile.map(line => (line.split("\\|")(0).toInt,line.split("\\|")(1)))
      val suppilerBroadcast = sc.broadcast(supplier.collectAsMap)
      
      val textFile = sc.textFile(args.input() + "/lineitem.tbl")
      val result = textFile
      .map(line => (line.split("\\|")(0).toInt,line.split("\\|")(1).toInt,line.split("\\|")(2).toInt,line.split("\\|")(10)))
      .filter(_._4.contains(date))
      .map(line =>{
          val orderKey = line._1
          val partKey = line._2
          val suppKey = line._3
          val partTable = partBroadcast.value
          val suppTable = suppilerBroadcast.value
          (orderKey,(partTable(partKey),suppTable(suppKey)))
      })
      .sortByKey()
      .take(20)
      .map(line => (line._1,line._2._1,line._2._2))
      .foreach(println) 

    } else if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
  	  val part = partRDD
  			.map(line => (line.getInt(0),line.getString(1)))
      val partBroadcast = sc.broadcast(part.collectAsMap)
  			
      val suppDF = sparkSession.read.parquet(args.input() + "/supplier")
      val suppRDD = suppDF.rdd
  	  val supp = suppRDD
  			.map(line => (line.getInt(0),line.getString(1)))
      val suppBroadcast = sc.broadcast(supp.collectAsMap)

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
  	  val result = lineitemRDD
  			.map(line => (line.getInt(0),line.getInt(1),line.getInt(2),line.getString(10)))
  			.filter(_._4.contains(date))
            .map(line =>{
                val orderKey = line._1
                val partKey = line._2
                val suppKey = line._3
                val partTable = partBroadcast.value
                val suppTable = suppilerBroadcast.value
                (orderKey,(partTable(partKey),suppTable(suppKey)))
            })
            .sortByKey()
            .take(20)
            .map(line => (line._1,line._2._1,line._2._2))
            .foreach(println) 
    }
	}
}
