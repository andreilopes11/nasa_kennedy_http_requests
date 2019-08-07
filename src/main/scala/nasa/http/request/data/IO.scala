package nasa.http.request.data

import org.apache.spark.sql.{DataFrame, SparkSession}

object IO {

  val session = SparkSession.builder()
    .appName("nasa-http-request")
    .getOrCreate()


  def readFile(path: String, sep : String): DataFrame = {
    return session.read.option("sep", sep).csv(path)
  }

  def saveFile(df: DataFrame, path: String): Unit = {
    df.write.csv(path)
  }

}

