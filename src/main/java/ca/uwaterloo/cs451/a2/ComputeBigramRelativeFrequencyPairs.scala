/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a2

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

class MyPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0)
  def numPartitions: Int = partitions
  def getPartition(key: Any): Int = key match {
    case null => 0
    case (key1,key2) => (key1.hashCode & Integer.MAX_VALUE) % numPartitions
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
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
    val textFile = sc.textFile(args.input(), args.reducers())
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
      .repartitionAndSortWithinPartitions(new MyPartitioner(args.reducers()))
      .reduceByKey(_ + _)
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
