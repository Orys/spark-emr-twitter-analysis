import DataManipulations._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


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

    // This creates a SparkSession, which will be used to operate on the DataFrames that we create.
    val spark = SparkSession.builder()
      .appName("spark-project")
      .config("spark.master", "local")
      .getOrCreate()


    //The SparkContext (usually denoted `sc` in code) is the entry point for low-level Spark APIs, including access to Resilient Distributed Datasets (RDDs).
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // defining the countries schema
    val countriesSchema = StructType(
      Array(
        StructField("country", StringType),
        StructField("country_code", StringType)
      )
    )

    // defining the tweets schema
    val tweetsSchema = StructType(
      Array(
        StructField("status_id", StringType),
        StructField("user_id", StringType),
        StructField("created_at", StringType, true),
        StructField("screen_name", StringType),
        StructField("text", StringType),
        StructField("source", StringType),
        StructField("reply_to_status_id", StringType),
        StructField("reply_to_user_id", StringType),
        StructField("reply_to_screen_name", StringType),
        StructField("is_quote", BooleanType, true),
        StructField("is_retweet", BooleanType, true),
        StructField("favourites_count", IntegerType, true),
        StructField("retweet_count", IntegerType, true),
        StructField("country_code", StringType),
        StructField("place_full_name", StringType),
        StructField("place_type", StringType),
        StructField("followers_count", IntegerType, true),
        StructField("friends_count", IntegerType, true),
        StructField("account_lang", StringType),
        StructField("account_created_at", StringType, true),
        StructField("verified", BooleanType, true),
        StructField("lang", StringType),
        StructField("sentiment", IntegerType, true)
      )
    )

    // read countries data as DataFrame
    val countriesDF = spark.read
      .option("header", "true")
      .schema(countriesSchema)
      .csv("src/main/resources/data/Countries.csv")
      //.csv("s3n://spark-project-test/twitter-data/Countries.CSV")

    // read tweets data as DataFrame and select useful columns
    val tweetsDF = spark.read
      //.option("inferSchema", "true")
      .schema(tweetsSchema)
      //.option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
      .parquet("src/main/resources/data/withSentiment.parquet")

      //.csv("src/main/resources/data/2020*.csv")
      //.csv("s3n://spark-project-test/twitter-data/2020*.CSV")
      //.csv("src/main/resources/data/2020-03-00_Covid_Tweets.csv")
      .select(
        "user_id",
        "screen_name",
        "verified",
        "country_code",
        "source",
        "followers_count",
        "friends_count",
        "favourites_count",
        "retweet_count",
        "created_at",
        "text",
        "lang",
        "sentiment"
      )
    //tweetsDF.printSchema()
    println("Rows in dataset: " + tweetsDF.count())
    tweetsDF.show()


    // clean DataFrame
    val cleanDF = tweetsDF
      .where(col("lang") === "en"
        and col("country_code") === "US"
        and col("created_at").isNotNull
        and col("followers_count").isNotNull
        and col("favourites_count").isNotNull
        and col("retweet_count").isNotNull
      )
      // change country_code with country by joining tweets and countries DataFrames
      .join(countriesDF, tweetsDF.col("country_code") === countriesDF.col("country_code")).drop("country_code").cache()

    println("Rows in dataset: " + cleanDF.count())
    cleanDF.show()

    // ordered by followers count
    println("Ordered by followers count")
    cleanDF.orderBy(desc("followers_count")).show()
    // ordered by retweet_count
    println("Ordered by retweet_count")
    cleanDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("retweet_count")).show
    // ordered by favourites_count
    println("Ordered by favourites_count")
    cleanDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("favourites_count")).show

    // tweetsDF -> tweetsRDD
    import spark.implicits._
    val tweetsRDD = cleanDF.as[Tweet].rdd.cache()

    // show most active users
    println("Most active users")
    namesNTweetsFollowersFriends(tweetsRDD).toDF.show

    // show countries with respective average sentiment
    println("Country and average sentiment")
    countryAvgSentiment(tweetsRDD).toDF.show()

    // show sentiments with respective ratios followers/favourites and followers/retweets
    println("Sentiment, followers/favourites, followers/retweets")
    sentimentFavouritesRetweetsRatios(tweetsRDD).toDF.show()

    // show the average sentiment for each day (created_at)
    println("Average sentiment of the date")
    avgSentimentDate(tweetsRDD).toDF.show()

    // show sentiments with respective number of tweets
    println("Number of tweets for each sentiment")
    sentimentNTweets(tweetsRDD).toDF.show()

    // show sentiments with respective number of tweets for each day (created_at)
    println("Number of tweets for each sentiment for each date")
    sentimentNTweetsDate(tweetsRDD).toDF.show()

  }
}
