package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output directory", required = true)
  verify()
}

object TrainSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args.input())

    // w is the weight vector (make sure the variable is within scope)
    val w = Map[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
    }

    // This is the main learner:
    val delta = 0.002

    // For each instance...
    val isSpam = ...   // label
    val features = ... // feature vector of the training instance

    // Update the weights as follows:
    val score = spamminess(features)
    val prob = 1.0 / (1 + exp(-score))
    features.foreach(f => {
        if (w.contains(f)) {
            w(f) += (isSpam - prob) * delta
        } else {
            w(f) = (isSpam - prob) * delta
        }
    })

    val trained = textFile.map(line =>{
        val tokens = line.split(" ")
        val doc = tokens(0)
        val isSpam
        if tokens(1) == "spam" {
            isSpam = 1d
        }else{
            isSpam = 0d
        }
        val features = tokens.drop(2).toInt
        
        (0, (docid, isSpam, features))
        }).groupByKey(1)
        .flatMap(lines => {
            lines._2.foreach( tuples => {
                val isSpam = tuples._2
                val features = tuples._3
                val score = spamminess(features)
                val prob = 1.0 / (1 + exp(-score))
                features.foreach(f => {
                    if (w.contains(f)) {
                        w(f) += (isSpam - prob) * delta
                    } else {
                        w(f) = (isSpam - prob) * delta
                    }
                })
            })
            w
        })
        // Then run the trainer...

    trained.saveAsTextFile(args.model())
    
	}
}
