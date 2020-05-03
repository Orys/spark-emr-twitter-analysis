import org.apache.spark.rdd.RDD
import SentimentAnalyzer._
import Main.Tweet

object DataManipulations {

  // name, number of tweets, followers and friends
  case class NamesNTweetsFollowersFriends(screen_name: String, country: String, tweets: Int, followers: Int, friends: Int)

  def namesNTweetsFollowersFriends (dataRDD: RDD[Tweet]): RDD[NamesNTweetsFollowersFriends] =
    dataRDD.groupBy(_.screen_name).map {
    case (screen_name, tweets) => NamesNTweetsFollowersFriends(
      screen_name, tweets.map(_.country).head,
      tweets.size, tweets.map(_.followers_count).head,
      tweets.map(_.friends_count).head
    )
  }

  // source and average of words in tweets
  case class SourceAvgWords(source: String, tweets: Int, words: Int)

  def sourceAvgWords (dataRDD: RDD[Tweet]): RDD[SourceAvgWords] =
    dataRDD.groupBy(_.source).map {
      case (source, tweets) => SourceAvgWords(
      source,
      tweets.size,
      tweets.map(_.text.split(" ").length).sum / (tweets.size+1)
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

  // add sentiment to data
  case class WithSentiment (user_id: String, name: String, text: String, created_at: String,  country: String, source: String,
                            followers_count: Int, favourites_count: Int, retweet_count: Int, sentiment: Int)
  def withSentiment (dataRDD: RDD[Tweet]): RDD[WithSentiment] = dataRDD.map(
    tweet => WithSentiment(
      tweet.user_id,
      tweet.screen_name,
      tweet.text,
      tweet.created_at,
      tweet.country,
      tweet.source,
      tweet.followers_count,
      tweet.favourites_count,
      tweet.retweet_count,
      mainSentiment(tweet.text)
    )
  )

  // sentiment, followers/favourites, followers/retweets
  case class SentimentFavouritesRetweetsRatios (sentiment: Int, r_follow_fav: Double, r_follow_retweet: Double)
  def sentimentFavouritesRetweetsRatios(dataRDD: RDD[Tweet]): RDD[SentimentFavouritesRetweetsRatios] =
    withSentiment(dataRDD).groupBy(_.sentiment).map{
      case (sentiment, tweets) => SentimentFavouritesRetweetsRatios(
        sentiment,
        tweets.map(tweet => tweet.followers_count/tweet.favourites_count).sum / (tweets.size+1),
        tweets.map(tweet => tweet.followers_count/tweet.retweet_count).sum / (tweets.size+1)
      )
    }.sortBy(_.sentiment)

  // get country and avgSentiment
  case class CountryAvgSentiment (country: String, avgSentiment: Int)
  def countryAvgSentiment (dataRDD: RDD[Tweet]): RDD[CountryAvgSentiment] =
    withSentiment(dataRDD).groupBy(_.country).map{
      case (country_code, withSentiments) => CountryAvgSentiment(
        country_code,
        withSentiments.map(_.sentiment).sum / (withSentiments.size+1)
      )
    }

}
