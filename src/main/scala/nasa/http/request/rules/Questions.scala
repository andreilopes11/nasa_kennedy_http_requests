package nasa.http.request.rules
import org.apache.spark.sql.{DataFrame, functions => func}
import org.apache.spark.sql.types._

object Questions {

  //1. Número​ ​ de​ ​ hosts​ ​ únicos.

  def HostNumbers(df: DataFrame) : Unit = {

    val qtde =  df.select("host").distinct().count()
    print("Número de hosts únicos")
    print("\n" + qtde.toString)
  }

  //O​ ​ total​ ​ de​ ​ erros​ ​ 404.

  def Erro404(df: DataFrame) : Unit = {
    val qtde =  df.filter(df("codigo").cast(LongType) === 404).count()
    print("\n\n O total de erros 404")
    print("\n" + qtde.toString)
  }

  //Os​ ​ 5 ​ ​ URLs​ ​ que​ ​ mais​ ​ causaram​ ​ erro​ ​ 404

  def UrlErro(df : DataFrame): Unit = {
    val dfFilter = df.filter(df("codigo").cast(LongType) === 404)

    val df5mais = dfFilter.select("host").groupBy("host").count()
      .sort(func.col("count").desc).limit(5)
    print("Os 5 url que mais causaram erro 404\n")
    df5mais.show()

  }

//  Quantidade de erros pordia
  def ErroporDia(df : DataFrame) : Unit = {
    val dfFilter = df.filter(df("codigo").cast(LongType) === 404)

    var dfDia = dfFilter.withColumn("dia", df("timestamp").substr(2, 11))

    dfDia = dfDia.select("dia").groupBy("dia").count()

    print("\n\nQuantidade de erros por dia\n")

    dfDia.show()
  }

//  Total de bytes retornado
  def TotalBytes(df: DataFrame): Unit ={

    print("Total de bytes retornado")
    df.agg(func.sum("bytes").alias("Soma")).show()
  }
}
