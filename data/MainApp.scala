import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{col, count, countDistinct, date_trunc, first, from_json, size, to_date, when}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import java.util.Date

object MainApp {
  case class EnabledDisabled(enabled: Seq[String], disabled: Seq[String])

  case class Token(vendors: EnabledDisabled, purposes: EnabledDisabled){
    def isPositive: Boolean ={
      purposes.enabled.nonEmpty
    }
  }

  case class User(country: String, id: String, token: String)

  case class Root(datetime: String, domain: String, id: String, `type`: String, user: User, user_token: Token, user_consent: Boolean)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Didomi")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.json("input/")

    val df_test = df.withColumn("user_token", from_json(col("user.token"), Encoders.product[Token].schema))
    val df_test2 = df_test.withColumn("user_consent", when(size(df_test("user_token.purposes.enabled")) > 0,true).otherwise(false))
      .withColumn("date", date_trunc("Hour", col("datetime")))



    val df_unique = df_test2.dropDuplicates("id").as[Root]
    df_unique.printSchema
    println("Initial count:"+ df_test2.count)
    println("Unique count:"+ df_unique.count)

    df_unique.show(50, false)

    val df_res_1 = df_unique.groupBy("date", "domain", "user.country")
      .agg(count(when(col("type") === "pageview", 1)).as("pageviews"))

    val df_res_2 = df_unique.groupBy("date", "domain", "user.country")
      .agg(count(when(col("type") === "pageview" && col("user_consent") === true, 1)).as("pageviews_with_consent"))

    val df_res_3 = df_unique.groupBy("date", "domain", "user.country")
      .agg(count(when(col("type") === "consent.asked", 1)).as("consents_asked"))

    val df_res_4 = df_unique.groupBy("date", "domain", "user.country")
      .agg(count(when(col("type") === "consent.asked" && col("user_consent") === true, 1)).as("consents_asked_with_consent"))

    val df_res_5 = df_unique.groupBy("date", "domain", "user.country")
      .agg(count(when(col("type") === "consent.given", 1)).as("consents_given"))

    val df_res_6 = df_unique.groupBy("date", "domain", "user.country")
      .agg(count(when(col("type") === "consent.given" && col("user_consent") === true, 1)).as("consents_given_with_consent"))

    val df_res_7 = df_unique.groupBy("date", "domain", "user.country")
      .agg(count(when(col("type") === "pageview", 1)).as("pageviews"), countDistinct("user.id").as("distinctUser"))
      .groupBy("date", "domain", "country")
      .agg((first(col("pageviews")) / first(col("distinctUser"))).as("avg_pageviews_per_user"))
      .drop("pageviews", "distinctUser")

    val df_final = df_res_1.join(df_res_2, Seq("date", "domain", "country"), "inner")
      .join(df_res_3, Seq("date", "domain", "country"), "inner")
      .join(df_res_4, Seq("date", "domain", "country"), "inner")
      .join(df_res_5, Seq("date", "domain", "country"), "inner")
      .join(df_res_6, Seq("date", "domain", "country"), "inner")
      .join(df_res_7, Seq("date", "domain", "country"), "inner")

    df_final.show(50, false)

  }

}
