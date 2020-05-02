import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import DataManipulations._

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
                    lang: String
                  )


  def main(args: Array[String]) {
    /**
     * This creates a SparkSession, which will be used to operate on the DataFrames that we create.
     */
    val spark = SparkSession.builder()
      .appName("spark-project")
      .config("spark.master", "local")
      .getOrCreate()

    /**
     * The SparkContext (usually denoted `sc` in code) is the entry point for low-level Spark APIs, including access to Resilient Distributed Datasets (RDDs).
     */
    val sc = spark.sparkContext

    import spark.implicits._

    // defining the schemas
    val countriesSchema = StructType(
      Array(
        StructField("country", StringType),
        StructField("country_code", StringType)
      )
    )

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


    val countriesDF = spark.read
      .option("header", "true")
      .schema(countriesSchema)
      .csv("src/main/resources/data/Countries.csv")
      //.csv("s3n://spark-project-test/twitter-data/Countries.CSV")


    val tweetsDF = spark.read
      .option("header", "true")
      .schema(tweetsSchema)
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
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
        "lang"
      )

    val cleanDF = tweetsDF
      .where(col("text").isNotNull
        and col("created_at").isNotNull
        and col("country_code") === "US"
        and col("lang") === "en"
      )
      .join(countriesDF, tweetsDF.col("country_code") === countriesDF.col("country_code")).drop("country_code")

    println(tweetsDF.count())

    cleanDF.show()

    // most active users
    cleanDF.groupBy("screen_name").agg(
      approx_count_distinct("text").as("nTweets")
    ).orderBy(desc("nTweets")).show

    tweetsDF.sort(desc("followers_count")).limit(20).show()

    cleanDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("retweet_count")).show
    cleanDF.select("screen_name", "text", "retweet_count", "favourites_count").orderBy(desc("favourites_count")).show

    val tweetsRDD = cleanDF.as[Tweet].rdd

    val withSentimentRDD = withSentiment(tweetsRDD)
    withSentimentRDD.toDF.show()
  }
}
