import SentimentAnalyzer.withSentiment

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._


object Main {

  /**
   * Define case classes for data
   */

  // case class for data
  case class Tweet(
                    status_id: String,
                    user_id: String,
                    created_at: String,
                    screen_name: String,
                    text: String,
                    followers_count: Int,
                    favourites_count: Int,
                    retweet_count: Int,
                    country_code: String,
                    lang: String
                  )

  // case class for data with sentiment
  case class WithSentiment(
                            status_id: String,
                            user_id: String,
                            created_at: String,
                            screen_name: String,
                            text: String,
                            followers_count: Int,
                            favourites_count: Int,
                            retweet_count: Int,
                            sentiment: Int
                          )

  /**
   * Define case classes for output functions
   */

  case class TweetsPerDay(created_at: String, n_tweets: Int, avg_sentiment: Double)

  case class MostActiveUsers(user_name: String, n_tweets: Int)

  case class MostPopularTweets(tweet_id: String, user_name: String, text: String, retweets: Int, likes: Int)

  case class MostPopularUsers(user_name: String, avg_retweets: Int, avg_likes: Int)

  case class WordCount(word: String, count: Int)

  case class HashtagOfTheDay(date: String, hashtag: String, n_tweets: Int)

  case class MostUsedHashtags(hashtag: String, n_tweets: Int)

  case class MostTaggedUsers(tag: String, n_tweets: Int)

  case class SortedByAvgSentiment(user_name: String, avg_sentiment: Double, n_tweets: Int)

  case class InfluentialSentiments(sentiment: Int, ratio_retweets_followers: Double, ratio_likes_followers: Double)

  case class MostRewardedUsers(user: String, obtained_followers: Int)


  /**
   * Main function
   */
  def main(args: Array[String]) {

    /**
     * This creates a SparkSession, which will be used to operate on the DataFrames that we create.
     */

    val spark = SparkSession.builder()
      .appName("spark-project")
      .config("spark.master", "local")
      .getOrCreate()

    /**
     * The SparkContext (usually denoted `sc` in code) is the entry point for low-level Spark APIs
     */

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR") // suppress warnings
    import spark.implicits._ // used for case classes

    /**
     * Define schemas
     */

    // schema for data
    val tweetsSchema = StructType(
      Array(
        StructField("status_id", StringType),
        StructField("user_id", StringType),
        StructField("created_at", DateType, nullable = true),
        StructField("screen_name", StringType),
        StructField("text", StringType),
        StructField("source", StringType),
        StructField("reply_to_status_id", StringType),
        StructField("reply_to_user_id", StringType),
        StructField("reply_to_screen_name", StringType),
        StructField("is_quote", BooleanType, nullable = true),
        StructField("is_retweet", BooleanType, nullable = true),
        StructField("favourites_count", IntegerType, nullable = true),
        StructField("retweet_count", IntegerType, nullable = true),
        StructField("country_code", StringType),
        StructField("place_full_name", StringType),
        StructField("place_type", StringType),
        StructField("followers_count", IntegerType, nullable = true),
        StructField("friends_count", IntegerType, nullable = true),
        StructField("account_lang", StringType),
        StructField("account_created_at", DateType, nullable = true),
        StructField("verified", BooleanType, nullable = true),
        StructField("lang", StringType)
      )
    )

    // schema for data with sentiment
    val sentimentSchema = StructType(
      Array(
        StructField("status_id", StringType),
        StructField("user_id", StringType),
        StructField("created_at", DateType, nullable = true),
        StructField("screen_name", StringType),
        StructField("text", StringType),
        StructField("followers_count", IntegerType, nullable = true),
        StructField("favourites_count", IntegerType, nullable = true),
        StructField("retweet_count", IntegerType, nullable = true),
        StructField("sentiment", IntegerType, nullable = true)

      )
    )

    // Create FileSystem object from Hadoop Configuration
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Data with sentiment file path
    //val sentimentPath = "s3n://spark-project-test/twitter-data/Sentiment_Data.csv"
    val sentimentPath = "src/main/resources/data/Sentiment_Data.csv"

    // if data with sentiment file already exists, read it, else create it and return data with sentiment
    val withSentimentRDD =
      if (fs.exists(new Path(sentimentPath)))
      // read data with sentiment
      spark.read.schema(sentimentSchema).option("header", "true").csv(sentimentPath).as[WithSentiment].rdd
        else {
        /**
         * Data preprocessing
         */
        // read data
        val tweetsDF = spark.read
          .schema(tweetsSchema)
          .option("header", "true")
          .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
          //.csv("s3n://spark-project-test/twitter-data/2020*.CSV")
          .csv("src/main/resources/data/2020-03-00_Covid_Tweets.csv")
          .select(
            "status_id",
            "user_id",
            "created_at",
            "screen_name",
            "text",
            "source",

            "is_quote",
            "is_retweet",

            "followers_count",
            "friends_count",
            "favourites_count",
            "retweet_count",

            "country_code",
            "lang"
          ).dropDuplicates("status_id") //remove duplicate tweets
          .where(
            col("text").isNotNull and // remove nulls
              col("created_at").isNotNull and // remove nulls
              col("is_quote") === "false" and // remove quotes [we only keep OC]
              col("is_retweet") === "false" and // remove retweets
              col("country_code") === "US" and // take only US tweets
              col("lang") === "en" // take only english tweets
          ).select(
          "status_id",
          "user_id",
          "created_at",
          "screen_name",
          "text",
          "source",

          "followers_count",
          "friends_count",
          "favourites_count",
          "retweet_count",

          "country_code",
          "lang"
        ) //.limit(1000)

        tweetsDF.printSchema()
        println("Data rows: " + tweetsDF.count())
        tweetsDF.show()

        /**
         * Sentiment Analysis
         */

        val sentimentRDD = withSentiment(tweetsDF.as[Tweet].rdd).filter(_.sentiment != -1).cache()
        sentimentRDD.toDF.write.option("header", "true").csv(sentimentPath)
        sentimentRDD

      }

    println("Clean data rows: " + withSentimentRDD.count())
    withSentimentRDD.toDF.show()

    /**
     * 1. Tweets per day and average sentiment
     */

    println("1. Tweets per day and average sentiment")
    val tweetsPerDay = withSentimentRDD.map(tweet => (tweet.created_at, (1, tweet.sentiment)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(tweet => TweetsPerDay(tweet._1, tweet._2._1, tweet._2._2.toDouble / tweet._2._1))
      .sortBy(_.created_at, ascending = true)

    tweetsPerDay.toDF.show()

    /**
     * 2. Most popular tweets
     */

    val mostPopularTweets = withSentimentRDD.map(row => MostPopularTweets(row.status_id, row.screen_name, row.text, row.retweet_count, row.favourites_count))
    println("2a. Most popular tweets - sorted by retweets")
    mostPopularTweets.sortBy(_.retweets, ascending = false).toDF.show()
    println("2b. Most popular tweets - sorted by likes")
    mostPopularTweets.sortBy(_.likes, ascending = false).toDF.show()

    /**
     * 3. Most popular users
     */

    val mostPopularUsers = withSentimentRDD.map(row => (row.screen_name, (row.retweet_count, row.favourites_count)))
      .reduceByKey((el1, el2) => (el1._1 + el2._1, el1._2 + el2._2))
      .map(row => MostPopularUsers(row._1, row._2._1, row._2._2))

    println("3a. Most popular users - sorted by retweets")
    mostPopularUsers.sortBy(_.avg_retweets, ascending = false).toDF.show()
    println("3b. Most popular users - sorted by likes")
    mostPopularUsers.sortBy(_.avg_likes, ascending = false).toDF.show()

    /**
     * 4. Most active users in the period and obtained followers
     */


    println("4a. Most active users")
    val mostActiveUsers = withSentimentRDD.map(tweet => (tweet.screen_name, 1))
      .reduceByKey(_ + _)
      .map(row => MostActiveUsers(row._1, row._2))
      .sortBy(_.n_tweets, ascending = false)

    mostActiveUsers.toDF.show()

    println("4b. Users who obtained more followers")
    val tweetsDateFollowers = withSentimentRDD.map(tweet => (tweet.screen_name, (tweet.created_at ,tweet.followers_count)))

    val firstTweets = tweetsDateFollowers.reduceByKey((el1, el2) => if (el1._1 < el2._1) el1 else el2).map(row => (row._1, row._2._2))
    val lastTweets = tweetsDateFollowers.reduceByKey((el1, el2) => if (el1._1 > el2._1) el1 else el2).map(row => (row._1, row._2._2))

    val mostRewardedUsers = firstTweets.join(lastTweets).map(row => MostRewardedUsers(row._1, row._2._2 - row._2._1)).sortBy(_.obtained_followers, ascending = false)
    mostRewardedUsers.toDF.show()


    /**
     * 5. Most common words in tweets [WordCount]
     */

    val stopwordsRDD = sc.textFile("src/main/resources/stopwords.txt").collect.toList

    val wordsRDD = withSentimentRDD.map(_.text.toLowerCase())
      .flatMap(_.split(" "))
      .map(word => (word.replaceAll("[^a-zA-Z#@0-9]", ""), 1))
      .filter(row => !(row._1 == ""))
      .reduceByKey((w1, w2) => w1 + w2)
      .filter(row => !stopwordsRDD.contains(row._1))
      .map(row => WordCount(row._1, row._2))
      .sortBy(_.count, ascending = false)

    val noHashtagsAndTagsRDD = wordsRDD.filter(!_.word.startsWith("#")).filter(!_.word.startsWith("@"))
    println("5. Most common words in tweets")
    noHashtagsAndTagsRDD.toDF.show()

    /**
     * 6. Most used hashtags
     **/

    println("6. Most used hashtags")
    val hashtagsRDD = wordsRDD
      .filter(_.word.startsWith("#"))
      .filter(_.word != "#")
      .map(row => MostUsedHashtags(row.word, row.count))
      .sortBy(_.n_tweets, ascending = false)

    hashtagsRDD.toDF.show()

    /**
     * 7. Most tagged users
     **/

    println("7. Most tagged users")
    val tagsRDD = wordsRDD
      .filter(_.word.startsWith("@"))
      .filter(_.word != "@")
      .map(row => MostTaggedUsers(row.word, row.count))
      .sortBy(_.n_tweets, ascending = false)

    tagsRDD.toDF.show()

    /**
     * 8. Most used hashtag of the day
     */

    println("8. Most used hashtag of the day")
    val hashtagOfTheDayRDD = withSentimentRDD.map(tweet => (tweet.created_at, tweet.text))
      .flatMapValues(text => text.split(" "))
      .filter(_._2.startsWith("#"))
      .map(row => ((row._1, row._2), 1))
      .reduceByKey(_ + _)
      .map(row => (row._1._1, (row._1._2, row._2)))
      .reduceByKey((el1, el2) => if (el1._2 > el2._2) el1 else el2)
      .sortByKey(ascending = true)
      .map(row => HashtagOfTheDay(row._1, row._2._1, row._2._2))

    hashtagOfTheDayRDD.toDF.show

    /**
     * 9. Most negative/positive users
     */

    val sortedByAvgSentiment = withSentimentRDD.map(row => (row.screen_name, 1))
      .reduceByKey(_ + _)
      .join(withSentimentRDD
        .map(row => (row.screen_name, row.sentiment))
        .reduceByKey(_ + _))
      .map(row => SortedByAvgSentiment(
        row._1,
        row._2._2.toDouble / row._2._1,
        row._2._1)
      )
      .filter(_.n_tweets > 8)

    println("9a. Most negative users")
    sortedByAvgSentiment.sortBy(_.avg_sentiment, ascending = true).toDF.show
    println("9b. Most positive users")
    sortedByAvgSentiment.sortBy(_.avg_sentiment, ascending = false).toDF.show

    /**
     * 10. Sentiments with ratios retweets/followers and likes/followers
     */

    println("10. Sentiments with ratios retweets/followers and likes/followers")
    val influentialSentimentsRDD = withSentimentRDD
      .map(row => (row.sentiment, (row.followers_count, row.retweet_count, row.favourites_count)))
      .reduceByKey((el1, el2) => (el1._1 + el2._1, el1._2 + el2._2, el1._3 + el2._3))
      .sortByKey(ascending = true)
      .map(row => InfluentialSentiments(row._1, row._2._2.toDouble / row._2._1, row._2._3.toDouble / row._2._1))

    influentialSentimentsRDD.toDF.show

  }
}
