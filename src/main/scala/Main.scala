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

    sc.setLogLevel("ERROR")



    import spark.implicits._

    // defining the schemas
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
      .csv("src/main/resources/data/Countries.CSV")
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
      .csv("src/main/resources/data/2020-03-12_Covid_Tweets.csv")
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
        "lang"
      )where(col("text").isNotNull
      and col("created_at").isNotNull
      and col("country_code").isNull)

    /**
     * How many tweets in english?
     * */

    val enTweetsDF = tweetsDF.where(col("lang") === "en")
      .sort(desc("followers_count"))

    println("Tweets in english - Size: " + enTweetsDF.count())
    enTweetsDF.show()


    /**
     * Who are the most followed?
     * */

    val mostFollowedDF = tweetsDF.groupBy("screen_name")
      .agg(max("followers_count").as("followers"))
      .sort(desc("followers"))

    println("Most followed users in full dataset - Size: " + mostFollowedDF.count())
    mostFollowedDF.show()

    /**
     * How many profiles have lang null and country null
     * */

    val nullUsers = tweetsDF.where(col("country_code").isNull
      and col("lang") === "en")
      .groupBy("screen_name")
      .agg(max("followers_count").as("nFollowers"))
      .sort(desc("nFollowers"))

    println("Users without lang - Size: " + nullUsers.count())// + " - tot tweets: " + nullUsers.agg(sum("nTweets")).first.get(0))
    nullUsers.show()



    // clean DataFrame
    val cleanDF = tweetsDF
      .where(col("lang") === "en"
        and col("country_code") === "US"
        and col("created_at").isNotNull
        and col("followers_count").isNotNull
        and col("favourites_count").isNotNull
        and col("retweet_count").isNotNull
      .where(col("text").isNotNull
        and col("created_at").isNotNull
        //and col("country_code") === "US"
        //and col("lang") === "en"
      )
      // change country_code with country by joining tweets and countries DataFrames
      .join(countriesDF, tweetsDF.col("country_code") === countriesDF.col("country_code")).drop("country_code").cache()
      //.join(countriesDF, tweetsDF.col("country_code") === countriesDF.col("country_code")).drop("country_code")

    // Which is the most spoken language?
    val langDF = cleanDF.groupBy("lang")
      .agg(count("*") as "speakers")
      .sort(desc("speakers"))

    println("Most represented languages in the dataset - Size: " + langDF.count())
    langDF.show()

    // Which is the most represented country?
    val tweetsPerCountryDF = cleanDF.groupBy("country_code")
      .agg(count("*") as "nTweets")
      .sort(desc("nTweets"))

    println("Most represented countries in the dataset - Size: " + tweetsPerCountryDF.count())
    tweetsPerCountryDF.show()

    println("Rows in dataset: " + cleanDF.count())
    // trump tweets
    val trumpTweetsDF = cleanDF.where(col("screen_name") === "realDonaldTrump")
    trumpTweetsDF.show()

    // english tweets around the world
    //val englishTweetsPerCountryDF = langDF.join(tweetsPerCountryDF, langDF.col("lang") === tweetsDF.col("country_code"))


    println(cleanDF.count())
    cleanDF.show()

    // ordered by followers count
    println("Ordered by followers count")
    cleanDF.orderBy(desc("followers_count")).show()
    // ordered by retweet_count
    println("Ordered by retweet_count")
    // most active users
    cleanDF.groupBy("screen_name").agg(count("text").as("nTweets")).orderBy(desc("nTweets")).show
    // ordered by retweet_count
    cleanDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("retweet_count")).show
    // ordered by favourites_count
    println("Ordered by favourites_count")
    // ordered by favourites_count
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



    val tweetsRDD = cleanDF.as[Tweet].rdd
    println("Partitions: " + tweetsRDD.getNumPartitions)

    //val withSentimentRDD = withSentiment(tweetsRDD)
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
    sentimentNTweets(tweetsRDD).toDF.show()

    // show sentiments with respective number of tweets for each day (created_at)
    println("Number of tweets for each sentiment for each date")
    sentimentNTweetsDate(tweetsRDD).toDF.show()

  }
}
