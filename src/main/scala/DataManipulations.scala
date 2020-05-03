import Main.Tweet
import org.apache.spark.rdd.RDD

object DataManipulations {

  // name, number of tweets, followers and friends
  case class NamesNTweetsFollowersFriends(screen_name: String, country: String, tweets: Int, followers: Int, friends: Int)

  def namesNTweetsFollowersFriends (dataRDD: RDD[Tweet]): RDD[NamesNTweetsFollowersFriends] =
    dataRDD.groupBy(_.screen_name).map {
    case (screen_name, tweets) => NamesNTweetsFollowersFriends(
      screen_name, tweets.map(_.country).head,
      tweets.size, tweets.map(_.followers_count).max,
      tweets.map(_.friends_count).max
    )
  }.sortBy(_.tweets, false)

  // source and average of words in tweets
  case class SourceAvgWords(source: String, tweets: Int, words: Int)

  def sourceAvgWords (dataRDD: RDD[Tweet]): RDD[SourceAvgWords] =
    dataRDD.groupBy(_.source).map {
      case (source, tweets) => SourceAvgWords(
      source,
      tweets.size,
      tweets.map(_.text.split(" ").length).sum / tweets.size
    )
  }

  // date and number of tweets
  case class DateNTweets(date: String, nTweets: Int)

  def dateNTweets(dataRDD: RDD[Tweet]): RDD[DateNTweets] =
    dataRDD.groupBy(_.created_at).map {
      case (date, tweets) => DateNTweets(
        date,
        tweets.size
      )
    }.sortBy(_.date)


  /****************************************************************************/
  /**                           SENTIMENT ANALYSIS                           **/
  /****************************************************************************/

  // get countries with respective average sentiment
  case class CountryAvgSentiment (country: String, avgSentiment: Int)

  def countryAvgSentiment (dataRDD: RDD[Tweet]): RDD[CountryAvgSentiment] =
    dataRDD.groupBy(_.country).map{
      case (country_code, withSentiments) => CountryAvgSentiment(
        country_code,
        withSentiments.map(_.sentiment).sum / withSentiments.size
      )
    }

  // sentiments with respective ratios favourites/followers and retweets/followers
  case class SentimentFavouritesRetweetsRatios (sentiment: Int, r_follow_fav: Double, r_follow_retweet: Double)

  def sentimentFavouritesRetweetsRatios(dataRDD: RDD[Tweet]): RDD[SentimentFavouritesRetweetsRatios] =
    dataRDD.groupBy(_.sentiment).map{
      case (sentiment, tweets) => SentimentFavouritesRetweetsRatios(
        sentiment,
        tweets.map(tweet => if (tweet.followers_count != 0) tweet.favourites_count/tweet.followers_count else 0).sum / tweets.size,
        tweets.map(tweet => if (tweet.followers_count != 0) tweet.retweet_count/tweet.followers_count else 0).sum / tweets.size
      )
    }.sortBy(_.sentiment)


  // average sentiment for each day (created_at)
  case class AvgSentimentDate (date: String, avgSentiment: Int)

  def avgSentimentDate(dataRDD: RDD[Tweet]): RDD[AvgSentimentDate] =
    dataRDD.groupBy(_.created_at).map{
      case (date, tweets) => AvgSentimentDate(
        date,
        tweets.map(_.sentiment).sum / tweets.size
      )
    }


  // sentiment with respective number of tweets for each day (created_at)
  case class SentimentNTweetsDate (date: String, sentiment: Int, nTweets: Int)

  def sentimentNTweetsDate(dataRDD: RDD[Tweet]): RDD[SentimentNTweetsDate] =
    dataRDD.groupBy(tweet => (tweet.sentiment, tweet.created_at)).map{
      case ((sentiment, created_at), tweets) => SentimentNTweetsDate(
        created_at,
        sentiment,
        tweets.size
      )
    }.sortBy(row => (row.date, row.sentiment))

  // sentiment with respective number of tweets for each day (created_at)
  case class SentimentNTweets (sentiment: Int, nTweets: Int)

  def sentimentNTweets(dataRDD: RDD[Tweet]): RDD[SentimentNTweets] =
    dataRDD.groupBy(_.sentiment).map{
      case (sentiment, tweets) => SentimentNTweets(
        sentiment,
        tweets.size
      )
    }.sortBy(_.sentiment)

}
