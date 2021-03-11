/*import org.apache.spark.sql.{SparkSession, Column, DataFrame, Dataset, Row}

//import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import scala.util.Try
import java.net.URLDecoder*/

/*
import scala.io.Source
//import scala.collection._
import org.apache.spark.sql.DataFrame
import java.net.URLDecoder
import org.apache.spark.sql._
import org.apache.spark.sql.execution
//import org.apache.spark.sql.Column
//import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.Dataset
import scala.util.Try
import java.io.{File, PrintWriter}
*/

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions=>F,types=>T}
import java.net.URLDecoder.decode
import java.net.{URL,URLDecoder}
import java.lang.IllegalArgumentException
import java.io.{File,FileWriter}
import scala.collection.mutable.WrappedArray
import java.text.DecimalFormat

object calcRelevance {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()
  def main(args: Array[String]): Unit = {


    println("Hello, main")
    //readData
  }

  def readData(): Unit = {
    val csLogDF = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .load("hdfs:///labs/laba02/logs/part-*").toDF("UID", "timestamp", "URL")

    val driversDF = spark.read.json("/labs/laba02/autousers.json")
      .select(explode(col("autousers")).alias(("autousers")))

    val patternHost: scala.util.matching.Regex = """^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)""".r

    def udf_Decode = udf {
      (url: String) => Try ( URLDecoder.decode(url, "UTF-8") ).toOption match {
        case Some(_) => URLDecoder.decode(url, "UTF-8")
        case None => "unknown"
      }
    }

    def udf_getHost(pattern: scala.util.matching.Regex) = udf(
      (url: String) => pattern.findFirstIn(url) match {
        case Some(domain) => domain
        case None => "unknown"
      }
    )


    val fillData: Map[String, Any] = Map("URL" -> "Undefined", "UID" -> 0)
    val replaceData: Map[Any, Any] = Map("-" -> "0")

    val cleanedDF = csLogDF.na.fill(fillData)
      .na.replace("UID", replaceData)

    val parsedDFp = cleanedDF.filter(col("URL").startsWith("http"))

      .withColumn("host", udf_Decode('URL))
      .withColumn("host", udf_getHost(patternHost)(col("host")))
      .withColumn("host", regexp_replace(regexp_replace('host, "^(http|https)://", ""), """^www\.""",""))


    val joinedCS = parsedDFp.join(driversDF, col("UID") === col("autousers"), "left")

    val markedCS = joinedCS.selectExpr("UID", "case when autousers is null then 0 else 1 end as is_autouser", "host")

    val groupedCS = markedCS.groupBy("host").agg(sum("is_autouser").alias("autouser_sum"), count("*").alias("total_sum"))

    val totalClicks: Long = parsedDFp.count
    val totalAutoClicks: Long = 313527//markedCS.agg(sum('is_autouser)).collect()(0)

    val calcRelevance = udf { (totalClicks: Double, totalAutoClicks: Double, total_sum: Double, autouser_sum: Double) =>
      ( (autouser_sum / totalClicks) * (autouser_sum / totalClicks) ) / ( (total_sum / totalClicks) * (totalAutoClicks / totalClicks) )
    }

    val result = groupedCS
      .withColumn("relevance",
        round(calcRelevance(lit(totalClicks), lit(totalAutoClicks), 'total_sum, 'autouser_sum), 20)
      )
      .drop('autouser_sum).drop('total_sum)
      .orderBy(col("relevance").desc, 'host)

    val FileObj = new PrintWriter(new File("/data/home/marat.akhmetov/laba02_domains.txt"))

    val array = result.limit(200).collect

    for (i<-0 to array.length-1){
      FileObj.write(array(i).mkString("\t")+"\n")
    }

    FileObj.close()

  }




  def writeData(): Unit = {
    println("Hello, writeData")
  }




}