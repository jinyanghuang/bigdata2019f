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

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

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
          tokens.sliding(2).map(p => (p.head,Map(p.last->1)))
          
        } else List()
      })
        .reduceByKey((value1,value2)=>{
          value1 ++ value2.map{ case (k,v) => (k,v + value1.getOrElse(k,0))}
      },args.reducers())
      .sortByKey()
       .map(
         stripes => {
           val sum = stripes._2.values.foldLeft(0.0)((x,y)=> x + y)
           val result = stripes._2.map{case(k,v) => k ->(v/sum)}
           (stripes._1,result)
         })

    .saveAsTextFile(args.output())
  }
}
