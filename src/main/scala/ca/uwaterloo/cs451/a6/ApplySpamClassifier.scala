package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.exp

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model directory", required = true)
  verify()
}

object ApplySpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
	FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val model = sc.textFile(args.model()+"/part-00000")
                .map(line =>{
                    val tokens = line.substring(1,line.length()-1).split(",")
                    val feature = tokens(0).toInt
                    val score = tokens(1).toDouble
                    (feature, score)
                }).collectAsMap
    val modelBroadCast = sc.broadcast(model)


    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        val modelTable = modelBroadCast.value
        features.foreach(f => if (modelTable.contains(f)) score += modelTable(f))
        score
    }


    val predict = textFile.map(line =>{
        val tokens = line.split(" ")
        val docid = tokens(0)
        val isSpam = tokens(1)
    
        val features = tokens.drop(2).map(line => (line.toInt))
    
        val score = spamminess(features)
        val modelTable = modelBroadCast.value
        val prediction = if (score > 0) "spam" else "ham"
        (docid, isSpam, score, prediction)
    })
        // Then run the trainer...

    predict.saveAsTextFile(args.output())
    
	}
}
