package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.exp

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model directory", required = true)
  val method = opt[String](descr = "method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method" + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
	FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val model1 = sc.textFile(args.model()+"/part-00000")
                .map(line =>{
                    val tokens = line.substring(1,line.length()-1).split(",")
                    val feature = tokens(0).toInt
                    val score = tokens(1).toDouble
                    (feature, score)
                }).collectAsMap
    val modelBroadCast1 = sc.broadcast(model1)

    val model2 = sc.textFile(args.model()+"/part-00001")
                .map(line =>{
                    val tokens = line.substring(1,line.length()-1).split(",")
                    val feature = tokens(0).toInt
                    val score = tokens(1).toDouble
                    (feature, score)
                }).collectAsMap
    val modelBroadCast2 = sc.broadcast(model2)

    val model3 = sc.textFile(args.model()+"/part-00002")
                .map(line =>{
                    val tokens = line.substring(1,line.length()-1).split(",")
                    val feature = tokens(0).toInt
                    val score = tokens(1).toDouble
                    (feature, score)
                }).collectAsMap
    val modelBroadCast3 = sc.broadcast(model3)

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int], model_id: Int) : Double = {
        var score = 0d
        var modelTable = scala.collection.Map[Int, Double]()
        if (model_id == 1){
            modelTable = modelBroadCast1.value
        }else if (model_id == 2){
            modelTable = modelBroadCast2.value
        }else {
            modelTable = modelBroadCast3.value
        }
        features.foreach(f => if (modelTable.contains(f)) score += modelTable(f))
        score
    }

    if (args.method() == "average"){
        val predict = textFile.map(line =>{
        val tokens = line.split(" ")
        val docid = tokens(0)
        val isSpam = tokens(1)
    
        val features = tokens.drop(2).map(line => (line.toInt))
    
        val score1 = spamminess(features,1)
        val score2 = spamminess(features,2)
        val score3 = spamminess(features,3)
        val average = (score1 + score2 + score3)/3
        val prediction = if (average > 0) "spam" else "ham"
        (docid, isSpam, average, prediction)
        })
        // Then run the trainer...

        predict.saveAsTextFile(args.output())
    
	    }else{
        val predict = textFile.map(line =>{
        val tokens = line.split(" ")
        val docid = tokens(0)
        val isSpam = tokens(1)
    
        val features = tokens.drop(2).map(line => (line.toInt))
    
        val score1 = spamminess(features,1)
        val score2 = spamminess(features,2)
        val score3 = spamminess(features,3)
        val vote1 = if (score1 > 0) 1d else -1d
        val vote2 = if (score2 > 0) 1d else -1d
        val vote3 = if (score3 > 0) 1d else -1d
        val score = vote1 + vote2 + vote3
        val prediction = if (score > 0) "spam" else "ham"
        (docid, isSpam, score, prediction)
        })
        // Then run the trainer...

        predict.saveAsTextFile(args.output())
        }
    }
    
}
