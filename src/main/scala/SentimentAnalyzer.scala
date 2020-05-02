import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.convert.wrapAll._

object SentimentAnalyzer{

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

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
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  private def extractSentiments(text: String): List[(String, Int)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])))
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