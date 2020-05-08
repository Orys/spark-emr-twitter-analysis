import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object Playground extends App {

  val spark = SparkSession.builder()
    .appName("spark-project")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val filePath = "/home/ale/Downloads/others/USA-Geolocated-tweets-free-dataset-Followthehashtag/dashboard_x_usa_x_filter_nativeretweets.csv"
  case class Nicknames(screen_name: String)
  val dataSchema = StructType(
    Array(
      StructField("Nickname", StringType)
    )
  )

  val dataDF = spark.read
    .option("header", "true")
    .option("schema", "dataSchema")
    .csv(filePath)
    .select("Nickname")
      .withColumnRenamed("Nickname", "screen_name")
  dataDF.show

  import spark.implicits._
  val dataRDD = dataDF.as[Nicknames]

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

  val tweetsDF = spark.read
    .schema(tweetsSchema)
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    //.csv("s3n://spark-project-test/twitter-data/2020*.CSV")
    .csv("src/main/resources/data/2020*.csv")
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
        col("country_code").isNull  // take only US tweets
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
  ).as[Tweet]//.limit(1000)

  tweetsDF.show

  val tweetsRDD = tweetsDF.rdd


  val condition = tweetsDF.col("screen_name") === dataDF.col("screen_name")
  val resultDF = dataDF.join(tweetsDF, condition)
  println("Data rows: " + resultDF.count())
  resultDF.show()


}
