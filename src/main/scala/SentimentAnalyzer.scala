import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import scala.collection.convert.wrapAll._

object SentimentAnalyzer {

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, pos,  parse, sentiment")
  props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  // define case class for data

  case class Tweet(
                    status_id: String,
                    user_id: String,
                    created_at: String,
                    screen_name: String,
                    text: String,
                    source: String,
                    reply_to_status_id: String,
                    reply_to_user_id: String,
                    reply_to_screen_name: String,
                    is_quote: Option[Boolean],
                    is_retweet: Option[Boolean],
                    favourites_count: Option[Int],
                    retweet_count: Option[Int],
                    country_code: String,
                    place_full_name: String,
                    place_type: String,
                    followers_count: Option[Int],
                    friends_count: Option[Int],
                    account_lang: String,
                    account_created_at: String,
                    verified: Option[Boolean],
                    lang: String
                  )

  def main(args: Array[String]) {

    // This creates a SparkSession, which will be used to operate on the DataFrames that we create.
    val spark = SparkSession.builder()
      .appName("sentiment-analyzer")
      .config("spark.master", "local")
      //.config("spark.sql.parquet.writeLegacyFormat" , "true")
      .getOrCreate()


    // The SparkContext (usually denoted `sc` in code) is the entry point for low-level Spark APIs, including access to Resilient Distributed Datasets (RDDs).
    val sc = spark.sparkContext
    //sc.setLogLevel("ERROR")

    val tweetsSchema = StructType(
      Array(
        StructField("status_id", StringType),
        StructField("user_id", StringType),
        StructField("created_at", DateType),
        StructField("screen_name", StringType),
        StructField("text", StringType),
        StructField("source", StringType),
        StructField("reply_to_status_id", StringType),
        StructField("reply_to_user_id", StringType),
        StructField("reply_to_screen_name", StringType),
        StructField("is_quote", BooleanType),
        StructField("is_retweet", BooleanType),
        StructField("favourites_count", IntegerType),
        StructField("retweet_count", IntegerType),
        StructField("country_code", StringType),
        StructField("place_full_name", StringType),
        StructField("place_type", StringType),
        StructField("followers_count", IntegerType),
        StructField("friends_count", IntegerType),
        StructField("account_lang", StringType),
        StructField("account_created_at", DateType),
        StructField("verified", BooleanType),
        StructField("lang", StringType)
      )
    )

    val tweetsDF = spark.read
      .option("header", "true")
      .schema(tweetsSchema)
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
      //.csv("src/main/resources/data/2020*.csv")
      .csv("src/main/resources/data/2020-03-00_Covid_Tweets.csv")
      //.csv("s3n://spark-project-test/twitter-data/2020*.CSV")
      //.csv("src/main/resources/data/2020-03-00_Covid_Tweets.csv")
      .where(
        col("text").isNotNull
        and col("lang") === "en"
        and col("country_code") === "US"
        and col("created_at").isNotNull
      )//.limit(100)

    println("Rows in dataset: " + tweetsDF.count())
    tweetsDF.show()

    import spark.implicits._
    val outFile = "src/main/resources/data/withSentiment.parquet"
    val result = withSentiment(tweetsDF.as[Tweet].rdd)
    result.toDF.write.mode(SaveMode.Overwrite).save(outFile)
    result.toDF.show()
    println("Data successfully written to: " + outFile)

  }

  /**
   * add column with Sentiment Analysis results to data
   */

    case class WithSentiment(status_id: String,
                             user_id: String,
                             created_at: String,
                             screen_name: String,
                             text: String,
                             source: String,
                             reply_to_status_id: String,
                             reply_to_user_id: String,
                             reply_to_screen_name: String,
                             is_quote: Option[Boolean],
                             is_retweet: Option[Boolean],
                             favourites_count: Option[Int],
                             retweet_count: Option[Int],
                             country_code: String,
                             place_full_name: String,
                             place_type: String,
                             followers_count: Option[Int],
                             friends_count: Option[Int],
                             account_lang: String,
                             account_created_at: String,
                             verified: Option[Boolean],
                             lang: String,
                             sentiment: Int)
  def withSentiment(dataRDD: RDD[Tweet]): RDD[WithSentiment] = dataRDD.map(
    tweet => WithSentiment(
      tweet.status_id,
      tweet.user_id,
      tweet.created_at,
      tweet.screen_name,
      tweet.text,
      tweet.source,
      tweet.reply_to_status_id,
      tweet.reply_to_user_id,
      tweet.reply_to_screen_name,
      tweet.is_quote,
      tweet.is_retweet,
      tweet.favourites_count,
      tweet.retweet_count,
      tweet.country_code,
      tweet.place_full_name,
      tweet.place_type,
      tweet.followers_count,
      tweet.friends_count,
      tweet.account_lang,
      tweet.account_created_at,
      tweet.verified,
      tweet.lang,
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
    if (sentiments.isEmpty) 2
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