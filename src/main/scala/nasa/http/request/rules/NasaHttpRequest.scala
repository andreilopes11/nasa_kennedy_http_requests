package nasa.http.request.rules

import org.apache.spark.sql.{DataFrame, functions => func}

object NasaHttpRequest {

  def DropColumn(dataFrame: DataFrame): DataFrame = {
    return dataFrame.drop("_c1", "_c2", "_c3", "_c4")
  }

  def RenamedColumn(dataFrame: DataFrame): DataFrame = {
    return dataFrame.withColumnRenamed("_c0", "host")
        //.withColumnRenamed("_c3", "timestamp")
        .withColumnRenamed("_c5", "requisicao")
        .withColumnRenamed("_c6", "codigo")
        .withColumnRenamed("_c7", "bytes")
  }

  def newColumns(dataFrame: DataFrame) : DataFrame = {
    return dataFrame.withColumn("timestamp", func.concat(dataFrame("_c3"), dataFrame("_c4")))
  }

  def UnionJulAug(dfAug : DataFrame, dfJul: DataFrame) : DataFrame = {
    return dfJul.union(dfAug)
  }
}
