import SentimentAnalyzer._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Main {

  // define case class for data
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

  case class MostActiveUsers(user_id: String, screen_name: String, nTweets: Int, followers: Int)
  case class TweetsPerDay(created_at: String, n_tweets: Int, avg_sentiment: Double)
  case class HashtagOfTheDay(date: String, hashtag: String, n_tweets: Int)
  case class MostUsedHashtags(hashtag: String, n_tweets: Int)
  case class MostTaggedUsers(tag: String, n_tweets: Int)



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

    sc.setLogLevel("ERROR")

    /**
     * Schemas
     */

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

    // defining the tweets schema
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

    import spark.implicits._

    // Create FileSystem object from Hadoop Configuration
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // sentiment dataset path
        //val sentimentPath = "s3n://spark-project-test/twitter-data/Sentiment_Data.parquet"
    val sentimentPath = "src/main/resources/data/Sentiment_Data.csv"
    // This methods returns Boolean (true - if file exists, false - if file doesn't exist
    val fileExists = fs.exists(new Path(sentimentPath))

    val withSentimentRDD = if (fileExists)
      spark.read.schema(sentimentSchema).option("header", "true").csv(sentimentPath).as[WithSentiment].rdd
    else {
      /**
       * Data preprocessing
       */
      // read tweets data as DataFrame and select useful columns
      val tweetsDF = spark.read
        .schema(tweetsSchema)
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
        //.csv("s3n://spark-project-test/twitter-data/2020*.CSV")
        .csv("src/main/resources/data/2020-03-00_Covid_Tweets.csv")
        .select( // Select useful columns
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
        .where(col("text").isNotNull and
          col("created_at").isNotNull and // remove nulls
          col("is_quote") === "false" and // remove quotes [we only keep OC]
          col("is_retweet") === "false" and // remove retweets
          (col("country_code") === "US") and
          (col("lang") === "en")
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
      )//.limit(1000)

      tweetsDF.printSchema()
      println("Rows in dataset: " + tweetsDF.count())
      tweetsDF.show()

      /**
       * Sentiment Analysis
       */

      val sentimentRDD = withSentiment(tweetsDF.as[Tweet].rdd)
      sentimentRDD.toDF.write.option("header", "true").csv(sentimentPath)
      sentimentRDD
    }

    val dataRDD = withSentimentRDD.filter(_.sentiment != -1).cache()

    println("                                                           Clean data rows: " + dataRDD.count())

    dataRDD.toDF.show()


    /*

    // ordered by followers count
    println("Ordered by followers count")
    withSentimentDF.orderBy(desc("followers_count")).show()
    // ordered by retweet_count
    println("Ordered by retweet_count")
    // ordered by retweet_count
    withSentimentDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("retweet_count")).show
    // ordered by favourites_count
    println("Ordered by favourites_count")
    // ordered by favourites_count
    withSentimentDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("favourites_count")).show

     */

    // utils functions for counting in aggregations
    val increaseCounter: (Int, WithSentiment) => Int = (n: Int, tweet: WithSentiment) => n+1
    val sumPartitions: (Int, Int) => Int = (n1: Int, n2: Int) => n1+n2

    /**
     * 1. Tweets per day and average sentiment, ordered by date
     */

    println("Tweets per day and average sentiment, ordered by date")
    val tweetsPerDay = dataRDD.map(tweet => (tweet.created_at, (1, tweet.sentiment)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(tweet => TweetsPerDay(tweet._1, tweet._2._1, tweet._2._2.toDouble / tweet._2._1))
      .sortBy(_.created_at, ascending = true)

    tweetsPerDay.toDF.show()

    /**
     * 2. Most popular tweets
     */

    println("Most popular tweets")
    dataRDD.sortBy(_.retweet_count).toDF.show()


    /**
     * 3. Most active users in the period
     */

    println("Most active users")

    val getLatestTweet: (WithSentiment, WithSentiment) => WithSentiment = (t1: WithSentiment, t2: WithSentiment) =>
      if (t1.created_at > t2.created_at) t1 else t2

    val keyUserID = dataRDD.map(tweet => (tweet.user_id, tweet))
    keyUserID
      .reduceByKey(getLatestTweet)
      .join(keyUserID.aggregateByKey(0)(increaseCounter, sumPartitions))
      .map(row => MostActiveUsers(
        row._1,
        row._2._1.screen_name,
        row._2._2,
        row._2._1.followers_count
        )
      ).sortBy(_.nTweets, ascending =false).toDF.show()

    /**
     * 4. Most common words in tweets [WordCount]
     */

    println("Most common words in tweets")
    val stopwordsRDD = sc.textFile("src/main/resources/stopwords.txt").collect.toList

    val wordsRDD = dataRDD.map(_.text.toLowerCase())
      .flatMap(_.split(" "))
      .map(word => (word.replaceAll("[^a-zA-Z#@0-9]", ""), 1))
      .reduceByKey((w1,w2) => w1+w2)
      .filter(row => !stopwordsRDD.contains(row._1))
      .sortBy(_._2, ascending = false)

    val noHashtagsAndTagsRDD = wordsRDD.filter(!_._1.startsWith("#")).filter(!_._1.startsWith("@"))
    noHashtagsAndTagsRDD.toDF.show()

    /**
     * 5. Most used hashtags
     * */

    println("Most used hashtags")
    val hashtagsRDD = wordsRDD
      .filter(_._1.startsWith("#"))
        .map(row => MostUsedHashtags(row._1, row._2))
    hashtagsRDD.toDF.show()

    /**
     * 6. Most tagged users
     * */

    println("Most tagged users")
    val tagsRDD = wordsRDD
      .filter(_._1.startsWith("@"))
      .map(row => MostTaggedUsers(row._1, row._2))
    tagsRDD.toDF.show()

    /**
     * 7. Most used hashtag of the day
     */

    println("Most used hashtag of the day")
    val hashtagOfTheDayRDD = dataRDD.map(tweet => (tweet.created_at, tweet.text))
      .flatMapValues(text => text.split(" "))
      .filter(_._2.startsWith("#"))
      .map(row => ((row._1, row._2), 1))
      .reduceByKey(_+_)
      .map(row => (row._1._1, (row._1._2, row._2)))
      .reduceByKey((el1, el2) => if (el1._2>el2._2) el1 else el2)
      .sortByKey(ascending = true)
      .map(row => HashtagOfTheDay(row._1, row._2._1, row._2._2))
    hashtagOfTheDayRDD.toDF.show

    /**
     * TODO Most negative users, ordered by followers_count and number of tweets
     */

    /**
     * TODO Most positive users, ordered by followers_count and number of tweets
     */

    /**
     * TODO change country_code with country by joining tweets and countries DataFrames
     */


    // change country_code with country by joining tweets and countries DataFrames
    //tweetsDF.join(countriesDF, "country_code").drop("country_code").cache()
    //.join(countriesDF, "country_code").drop("country_code")
/*
    /**
     * Who are the most followed?
     * */

    val mostFollowedDF = withSentimentDF.groupBy("screen_name")
      .agg(max("followers_count").as("followers_count"))
      .sort(desc("followers_count"))

    println("Most followed users in full dataset - Size: " + mostFollowedDF.count())
    mostFollowedDF.show()


        /**
         * How many tweets in english?
         * */

        val enTweetsDF = withSentimentDF.where(col("lang") === "en")
          .sort(desc("followers_count"))

        println("Tweets in english - Size: " + enTweetsDF.count())
        enTweetsDF.show()


        /**
         * Data exploration
         * */

        val nullUsers = withSentimentDF.where(col("country_code").isNull
          and col("lang") === "en")
          .groupBy("screen_name")
          .agg(max("followers_count").as("nFollowers"))
          .sort(desc("nFollowers"))

        println("Users without lang - Size: " + nullUsers.count())// + " - tot tweets: " + nullUsers.agg(sum("nTweets")).first.get(0))
        nullUsers.show()

        // Which is the most spoken language?
        val langDF = withSentimentDF.groupBy("lang")
          .agg(count("*") as "speakers")
          .sort(desc("speakers"))

        println("Most represented languages in the dataset - Size: " + langDF.count())
        langDF.show()

        // Which is the most represented country?
        val tweetsPerCountryDF = withSentimentDF.groupBy("country_code")
          .agg(count("*") as "nTweets")
          .sort(desc("nTweets"))

        println("Most represented countries in the dataset - Size: " + tweetsPerCountryDF.count())
        tweetsPerCountryDF.show()

        println("Rows in dataset: " + withSentimentDF.count())
        // trump tweets
        val trumpTweetsDF = withSentimentDF.where(col("screen_name") === "realDonaldTrump")
        trumpTweetsDF.show()

        // english tweets around the world
        //val englishTweetsPerCountryDF = langDF.join(tweetsPerCountryDF, langDF.col("lang") === tweetsDF.col("country_code"))


        println(withSentimentDF.count())
        withSentimentDF.show()


        /**
         * working with RDD
         */
        // tweetsDF -> withSentimentRDD
        import spark.implicits._
        val withSentimentRDD = withSentimentDF.as[Tweet].rdd.cache()

        // show countries with respective average sentiment
        println("Country and average sentiment")
        countryAvgSentiment(withSentimentRDD).toDF.show()

        // show sentiments with respective ratios followers/favourites and followers/retweets
        println("Sentiment, followers/favourites, followers/retweets")
        sentimentFavouritesRetweetsRatios(withSentimentRDD).toDF.show()

        // show the average sentiment for each day (created_at)
        println("Average sentiment of the date")
        avgSentimentDate(withSentimentRDD).toDF.show()
    
        println("Partitions: " + withSentimentRDD.getNumPartitions)

        //val withSentimentRDD = withSentiment(withSentimentRDD)
        //withSentimentRDD.take(10).foreach(println)


        /*
        case class SentimentCount(sentiment: Int, nTweets: Int)
        val resultRDD = withSentimentRDD.groupBy(_.sentiment).map {
          case (sentiment, tweets) => SentimentCount(
            sentiment,
            tweets.size
          )
        }

        resultRDD.take(10).foreach(println)
        */

        //val sortedWithSentiment = withSentimentRDD.toDF().sort(desc("followers_count"))
        //sortedWithSentiment.take(20).foreach(println)
        //sortedWithSentiment.show()

        // show sentiments with respective number of tweets
        println("Number of tweets for each sentiment")
        sentimentNTweets(withSentimentRDD).toDF.show()

        // show sentiments with respective number of tweets for each day (created_at)
        println("Number of tweets for each sentiment for each date")
        sentimentNTweetsDate(withSentimentRDD).toDF.show()

*/
  }
}
