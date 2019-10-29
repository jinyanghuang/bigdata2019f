package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object Q1 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var sum = 0.0
    val textFile = sc.textFile(args.input())
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1){
          val pair = tokens.sliding(2).map(p => (p.head,p.last) ).toList
          val pairStar = tokens.init.sliding(1).map(q => (q.head,"*")).toList
          pair++pairStar
        }  else List()
      })
      .map(pair => (pair, 1))
      .reduceByKey(_ + _,args.reducers())
      .sortByKey()
      .map(
        pair => pair._1 match {
        case (_,"*") => {
           sum = pair._2
           (pair._1, pair._2)
        }
        case(_,_) => {
          (pair._1, pair._2/sum)
        }
        })

    .saveAsTextFile(args.output())
  }
}
