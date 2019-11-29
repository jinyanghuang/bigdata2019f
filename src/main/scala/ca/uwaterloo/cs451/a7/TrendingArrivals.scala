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

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext, Time, Seconds, StateSpec, State}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def trending(batchTime: Time, key: String, value: Option[Int], state: State[Int]): Option[(String, (Int, Long, Int))] = {
      val previousState = state.getOption.getOrElse(0)
      val currentState = value.getOrElse(0)
      if(currentState >= 10 && currentState >= 2*previousState){
          if(key == "citigroup"){
              log.info("Number of arrivals to Citigroup has doubled from " + previousState + " to " + currentState + " at " + batchTime.milliseconds + "!")
          }else if(key == "goldman"){
              log.info("Number of arrivals to Goldman has doubled from " + previousState + " to " + currentState + " at " + batchTime.milliseconds + "!")
          }
      }
      val output = (key, (currentState, batchTime.milliseconds, previousState))
      state.update(currentState)
      Some(output)
  }

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val stateSpec = StateSpec.function(trending _).numPartitions(2).timeout(Minutes(10))

    val wc = stream.map(_.split(","))
      .map(line => {
          if(line(0) == "yellow"){
              (line(10).toDouble, line(11).toDouble)
          }else{
              (line(8).toDouble, line(9).toDouble)
          }
      })
      .filter(p => ((p._1 >= -74.0144185 && p._1 <= -74.013777 && p._2 >= 40.7138745 && p._2 <= 40.7152275)||(p._1 >= -74.012083 && p._1 <= -74.009867 && p._2 >= 40.720053 && p._2 <= 40.7217236)))
      .map( p=> {
          if (p._1 >= -74.0144185 && p._1 <= -74.013777 && p._2 >= 40.7138745 && p._2 <= 40.7152275){
                  ("goldman" , 1)
          }
          else {
                  ("citigroup",1)
          }
      })
      
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .mapWithState(StateSpec.function(trending _))
      .persist()

    wc.saveAsTextFiles(args.output()+"/part-")

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
