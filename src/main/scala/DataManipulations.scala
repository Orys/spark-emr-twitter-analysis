import Main._
import org.apache.spark.rdd.RDD

object DataManipulations {

  val increaseCounter: (Int, WithSentiment) => Int = (n: Int, tweet: WithSentiment) => n+1
  val sumPartitions: (Int, Int) => Int = (n1: Int, n2: Int) => n1+n2

  /**
   * Date and number of tweets
   */
  case class DateNTweets(date: String, nTweets: Int)

  def dateNTweets: RDD[WithSentiment] => RDD[DateNTweets] = (dataRDD: RDD[WithSentiment]) =>
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
   * Sentiments with respective ratios favourites/followers and retweets/followers
   */
  case class SentimentFavouritesRetweetsRatios (sentiment: Int, r_follow_fav: Double, r_follow_retweet: Double)

  def sentimentFavouritesRetweetsRatios: RDD[WithSentiment] => RDD[SentimentFavouritesRetweetsRatios] = (dataRDD: RDD[WithSentiment]) =>
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

  def avgSentimentDate: RDD[WithSentiment] => RDD[AvgSentimentDate] = (dataRDD: RDD[WithSentiment]) =>
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

  def sentimentNTweetsDate: RDD[WithSentiment] => RDD[SentimentNTweetsDate] = (dataRDD: RDD[WithSentiment]) => {
    val result = dataRDD.map(tweet => ((tweet.created_at, tweet.sentiment), tweet))
      .aggregateByKey(0)(increaseCounter, sumPartitions)
    result.map(row => SentimentNTweetsDate(row._1._1, row._1._2, row._2))
  }.sortBy(row => (row.date, row.sentiment))


  /**
   * Sentiment with number of tweets for each day (created_at)
   */
  case class SentimentNTweets (sentiment: Int, nTweets: Int)

  def sentimentNTweets: RDD[WithSentiment] => RDD[SentimentNTweets] = (dataRDD: RDD[WithSentiment]) =>{
    val result = dataRDD.map(tweet => (tweet.sentiment, tweet))
      .aggregateByKey(0)(increaseCounter, sumPartitions)
    result.map(row => SentimentNTweets(row._1, row._2))
  }.sortBy(_.nTweets)

}
