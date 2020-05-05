import Main._
import org.apache.spark.rdd.RDD

object DataManipulations {

  val increaseCounter: (Int, Tweet) => Int = (n: Int, tweet: Tweet) => n+1
  val sumPartitions: (Int, Int) => Int = (n1: Int, n2: Int) => n1+n2



  /**
   * Source and average of words in tweets
   */
  case class SourceAvgWords(source: String, tweets: Int, words: Int)

  def sourceAvgWords: RDD[Tweet] => RDD[SourceAvgWords] = (dataRDD: RDD[Tweet]) =>
    dataRDD.groupBy(_.source).map {
      case (source, tweets) => SourceAvgWords(
      source,
      tweets.size,
      tweets.map(_.text.split(" ").length).sum / tweets.size
    )
  }

  /**
   * Date and number of tweets
   */
  case class DateNTweets(date: String, nTweets: Int)

  def dateNTweets: RDD[Tweet] => RDD[DateNTweets] = (dataRDD: RDD[Tweet]) =>
    dataRDD.groupBy(_.created_at).map {
      case (date, tweets) => DateNTweets(
        date,
        tweets.size
      )
    }.sortBy(_.date)


  /****************************************************************************/
  /**                           SENTIMENT ANALYSIS                           **/
  /****************************************************************************/

  /**
   * Get countries with respective average sentiment
   */
  case class CountryAvgSentiment (country_code: String, avgSentiment: Int)

  def countryAvgSentiment: RDD[Tweet] => RDD[CountryAvgSentiment] = (dataRDD: RDD[Tweet]) =>
    dataRDD.groupBy(_.country_code).map{
      case (country_code, withSentiments) => CountryAvgSentiment(
        country_code,
        withSentiments.map(_.sentiment).sum / withSentiments.size
      )
    }

  /**
   * Sentiments with respective ratios favourites/followers and retweets/followers
   */
  case class SentimentFavouritesRetweetsRatios (sentiment: Int, r_follow_fav: Double, r_follow_retweet: Double)

  def sentimentFavouritesRetweetsRatios: RDD[Tweet] => RDD[SentimentFavouritesRetweetsRatios] = (dataRDD: RDD[Tweet]) =>
    dataRDD.groupBy(_.sentiment).map{
      case (sentiment, tweets) => SentimentFavouritesRetweetsRatios(
        sentiment,
        tweets.map(tweet => if (tweet.followers_count != 0) tweet.favourites_count/tweet.followers_count else 0).sum / tweets.size,
        tweets.map(tweet => if (tweet.followers_count != 0) tweet.retweet_count/tweet.followers_count else 0).sum / tweets.size
      )
    }.sortBy(_.sentiment)


  /**
   * Average sentiment for each day (created_at)
   */
  case class AvgSentimentDate (date: String, avgSentiment: Int)

  def avgSentimentDate: RDD[Tweet] => RDD[AvgSentimentDate] = (dataRDD: RDD[Tweet]) =>
    dataRDD.groupBy(_.created_at).map{
      case (date, tweets) => AvgSentimentDate(
        date,
        tweets.map(_.sentiment).sum / tweets.size
      )
    }


  /**
   * Sentiment with number of tweets for each day (created_at)
   */
  case class SentimentNTweetsDate (date: String, sentiment: Int, nTweets: Int)

  def sentimentNTweetsDate: RDD[Tweet] => RDD[SentimentNTweetsDate] = (dataRDD: RDD[Tweet]) => {
    val result = dataRDD.map(tweet => ((tweet.created_at, tweet.sentiment), tweet))
      .aggregateByKey(0)(increaseCounter, sumPartitions)
    result.map(row => SentimentNTweetsDate(row._1._1, row._1._2, row._2))
  }.sortBy(row => (row.date, row.sentiment))


  /**
   * Sentiment with number of tweets for each day (created_at)
   */
  case class SentimentNTweets (sentiment: Int, nTweets: Int)

  def sentimentNTweets: RDD[Tweet] => RDD[SentimentNTweets] = (dataRDD: RDD[Tweet]) =>{
    val result = dataRDD.map(tweet => (tweet.sentiment, tweet))
      .aggregateByKey(0)(increaseCounter, sumPartitions)
    result.map(row => SentimentNTweets(row._1, row._2))
  }.sortBy(_.nTweets)

}
