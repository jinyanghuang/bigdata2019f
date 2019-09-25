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
import scala.collection.mutable.ListBuffer
import scala.math.log10

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "number of threshold", required = false, default = Some(10))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val wordCount = textFile
      .flatMap(line => {
        tokenize(line).take(Math.min(40, line.length)).distinct
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .collectAsMap()
    val broadcastWordCount = sc.broadcast(wordCount)
    var totalLines : Float = textFile.count()
    val threshold = args.threshold()
    textFile
     .flatMap(line => {
        val tokens = tokenize(line).take(Math.min(40, line.length)).distinct
        val occurrences = new ListBuffer[(String,String)]()
        if (tokens.length > 1){
          for (i <- tokens){
            for (j <- tokens){
                if(i!=j){  
                  occurrences+=((i,j))
                }
            }
          }
          occurrences.toList
        } else{
         List()
        }
        })
    
     .map(word => (word, 1))
   .reduceByKey(_ + _)
     .sortByKey()
     .filter(_._2 >= threshold)
     .map(pair => {
         var sum = pair._2
         var pmi = log10(pair._2 * totalLines/(broadcastWordCount.value(pair._1._1) * broadcastWordCount.value(pair._1._2)))
         (pair._1,(pmi,sum))
     })
    .saveAsTextFile(args.output())
  }
}
