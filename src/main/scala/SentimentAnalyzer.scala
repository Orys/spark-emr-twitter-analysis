import java.util.Properties

import Main.{Tweet, TweetWithSentiment}
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.rdd.RDD

import scala.collection.convert.wrapAll._

object SentimentAnalyzer {

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, pos,  parse, sentiment")
  props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  // define case class for data

  case class IDText(status_id: String, text: String)

  /**
   * add column with Sentiment Analysis results to data
   */

  def withSentiment(dataRDD: RDD[Tweet]): RDD[TweetWithSentiment] = dataRDD.map(
    tweet => TweetWithSentiment(
      tweet.status_id,
      tweet.user_id,
      tweet.created_at,
      tweet.screen_name,
      tweet.text,
      tweet.followers_count,
      tweet.favourites_count,
      tweet.retweet_count,
      mainSentiment(tweet.text)
    )
  )

  /**
   * Extracts the main sentiment for a given input
   */
  def mainSentiment(input: String): Int = Option(input) match {
    case Some(text) if text.nonEmpty => extractSentiment(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  /**
   * Extracts a list of sentiments for a given input
   */
  def sentiment(input: String): List[(String, Int)] = Option(input) match {
    case Some(text) if text.nonEmpty => extractSentiments(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  private def extractSentiment(text: String): Int = {
    val sentiments = extractSentiments(text)
    if (sentiments.isEmpty) -1
    else {
      val (_, sentiment) = extractSentiments(text)
        .maxBy { case (sentence, _) => sentence.length }
      sentiment
    }
  }

  private def extractSentiments(text: String): List[(String, Int)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, RNNCoreAnnotations.getPredictedClass(tree)) }
      .toList
  }
}

/*
object Sentiment extends Enumeration {

  type Sentiment = Value
  val VERY_POSITIVE, POSITIVE, NEUTRAL, NEGATIVE, VERY_NEGATIVE = Value

  def toSentiment(sentiment: Int): Sentiment = sentiment match {
    case 0 => Sentiment.VERY_NEGATIVE
    case 1 => Sentiment.NEGATIVE
    case 2 => Sentiment.NEUTRAL
    case 3 => Sentiment.POSITIVE
    case 4 => Sentiment.VERY_POSITIVE
  }

  def toInt(sentiment: Sentiment): Int = sentiment match {
    case VERY_NEGATIVE => 0
    case NEGATIVE => 1
    case NEUTRAL => 2
    case POSITIVE => 3
    case VERY_POSITIVE => 4
  }

  def toString(sentiment: Sentiment): String = sentiment match {
    case VERY_NEGATIVE => "VERY_NEGATIVE"
    case NEGATIVE => "NEGATIVE"
    case NEUTRAL => "NEUTRAL"
    case POSITIVE => "POSITIVE"
    case VERY_POSITIVE => "VERY_POSITIVE"
  }
}

 */