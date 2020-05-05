import Main.WithSentiment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Playground extends App {

  val spark = SparkSession.builder()
    .appName("spark-project")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._
  val sentimentPath = "src/main/resources/data/Sentiment_Data.csv"

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
  val dataRDD = spark.read.schema(sentimentSchema).option("header", "true").csv(sentimentPath).as[WithSentiment].rdd
  val stopwordsRDD = sc.textFile("src/main/resources/stopwords.txt").collect.toList



}
