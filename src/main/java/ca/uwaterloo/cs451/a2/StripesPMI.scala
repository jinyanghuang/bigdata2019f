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

    val conf = new SparkConf().setAppName("Stripes PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
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
        val occurrences = new ListBuffer[(String,Map[String,Int])]()
        if (tokens.length > 1){
          for (i <- tokens){
            for (j <- tokens){
                if(i!=j){  
                  occurrences+=((i,Map(j->1)))
                }
            }
          }
          occurrences.toList
        } else{
         List()
        }
        })
     .reduceByKey((value1,value2)=>{
          value1 ++ value2.map{ case (k,v) => (k,v + value1.getOrElse(k,0))}
      },args.reducers())
     .sortByKey()
     .map(stripes => { 
         (stripes._1, stripes._2.filter((pair) => pair._2 >= threshold).map{ 
             case (k,v) => {
         var pmi = log10(v * totalLines/(broadcastWordCount.value(k)*broadcastWordCount.value(stripes._1)))
        k + "=(" + pmi + "," + v +")"
         
     }})
  }
  ).filter(p=>{p._2.size > 0
  }).map(stripes =>"("+stripes._1+"," + "{" + (stripes._2 mkString ",")+ "})")
       .saveAsTextFile(args.output())
}
}
