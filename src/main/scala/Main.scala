import DataManipulations._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import SentimentAnalyzer._


object Main {

  // define case class for data
  case class Tweet(
                    user_id: String,
                    screen_name: String,
                    verified: Boolean,
                    country: String,
                    source: String,
                    followers_count: Int,
                    friends_count: Int,
                    favourites_count: Int,
                    retweet_count: Int,
                    created_at: String,
                    text: String,
                    lang: String,
                    sentiment: Int
                  )

  def main(args: Array[String]) {

    /**
     * This creates a SparkSession, which will be used to operate on the DataFrames that we create.
     */
    val spark = SparkSession.builder()
      .appName("spark-project")
      //.config("spark.master", "local")
      .getOrCreate()

    /**
     * The SparkContext (usually denoted `sc` in code) is the entry point for low-level Spark APIs
     */
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    /**
     * Schemas
     */

    // defining the countries schema
    val countriesSchema = StructType(
      Array(
        StructField("country", StringType),
        StructField("country_code", StringType)
      )
    )

    val sentimentSchema = StructType(
      Array(
        StructField("status_id", StringType),
        StructField("sentiment", IntegerType, nullable = true),
        StructField("user_id", StringType),
        StructField("created_at", DateType, nullable = true),
        StructField("screen_name", StringType),
        StructField("text", StringType),
        StructField("source", StringType),
        StructField("is_quote", BooleanType, nullable = true),
        StructField("is_retweet", BooleanType, nullable = true),
        StructField("followers_count", IntegerType, nullable = true),
        StructField("friends_count", IntegerType, nullable = true),
        StructField("favourites_count", IntegerType, nullable = true),
        StructField("retweet_count", IntegerType, nullable = true),
        StructField("country_code", StringType),
        StructField("lang", StringType)
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
        //StructField("sentiment", IntegerType, nullable = true)
      )
    )

    /**
     * Read data files and create DataFrames
     */

    // read countries data as DataFrame
    val countriesDF = spark.read
      .option("header", "true")
      .schema(countriesSchema)
      //.csv("src/main/resources/data/Countries.CSV")
      .csv("s3n://spark-project-test/twitter-data/Countries.CSV")

    /**
     * Data preprocessing
     */

    // read tweets data as DataFrame and select useful columns
    val tweetsDF = spark.read
      .schema(tweetsSchema)
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
      .csv("s3n://spark-project-test/twitter-data/2020*.CSV")
      //.csv("src/main/resources/data/2020-03-00_Covid_Tweets.csv")
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
        (col("country_code").isNull or col("country_code") === "US") and
        (col("lang").isNull or col("lang") === "en")
      )

    tweetsDF.printSchema()

    println("Rows in dataset: " + tweetsDF.count())

    tweetsDF.show()

    /**
     * Sentiment Analysis
     */

    import spark.implicits._

    // Create FileSystem object from Hadoop Configuration
    val fs = FileSystem.get(new java.net.URI(s"s3a://spark-project-test"), spark.sparkContext.hadoopConfiguration)
    // sentiment dataset path
    val sentimentPath = "s3n://spark-project-test/twitter-data/Sentiment_Data.parquet"
    // This methods returns Boolean (true - if file exists, false - if file doesn't exist
    val fileExists = fs.exists(new Path(sentimentPath))

    val withSentimentDF = if (fileExists)
      spark.read.schema(sentimentSchema).parquet(sentimentPath)
    else {
      val sentimentRDD = withSentiment(tweetsDF.select("status_id", "text").as[IDText].rdd).toDF.join(tweetsDF, "status_id")
      sentimentRDD.write.mode(SaveMode.Overwrite).save(sentimentPath)
      sentimentRDD.toDF()
    }
    println("Tweets with sentiment -1: " + withSentimentDF.where(col("sentiment") === -1).count())
    println("Tweets with null lang: " + withSentimentDF.where(col("lang").isNull).count())
    println("Tweets with null country code: " + withSentimentDF.where(col("country_code").isNull).count())


    val tweetsData = withSentimentDF.drop(col("sentiment") === -1)

    println("Clean data rows: " + tweetsData.count())

    /**
     * 1. Tweets per day, ordered by date
     */

    //val tweetsPerDayDF = ???

    /**
     * 2. Most popular tweets
     */

    /**
     * 3. Most active users in the period
     */

    /**
     * 4. Most common words in tweets [WordCount]
     */

    /**
     * 5. Average sentiment per day, ordered by sentiment
     */

    /**
     * 6. Most negative users, ordered by followers_count and number of tweets
     */

    /**
     * 7. Most positive users, ordered by followers_count and number of tweets
     */


    // change country_code with country by joining tweets and countries DataFrames
    //tweetsDF.join(countriesDF, tweetsDF.col("country_code") === countriesDF.col("country_code")).drop("country_code").cache()
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
         * working with DF
         */
        // ordered by followers count
        println("Ordered by followers count")
        withSentimentDF.orderBy(desc("followers_count")).show()
        // ordered by retweet_count
        println("Ordered by retweet_count")
        // most active users
        withSentimentDF.groupBy("screen_name").agg(count("text").as("nTweets")).orderBy(desc("nTweets")).show
        // ordered by retweet_count
        withSentimentDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("retweet_count")).show
        // ordered by favourites_count
        println("Ordered by favourites_count")
        // ordered by favourites_count
        withSentimentDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("favourites_count")).show


        /**
         * working with RDD
         */
        // tweetsDF -> withSentimentRDD
        import spark.implicits._
        val withSentimentRDD = withSentimentDF.as[Tweet].rdd.cache()

        // show most active users
        println("Most active users")
        namesNTweetsFollowersFriends(withSentimentRDD).toDF.show

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
