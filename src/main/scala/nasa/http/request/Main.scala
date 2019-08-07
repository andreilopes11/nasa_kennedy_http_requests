package nasa.http.request

import nasa.http.request.data.IO
import nasa.http.request.config.MetaInf
import nasa.http.request.rules.{NasaHttpRequest, Questions}

object Main extends App {
  val dfAug = IO.readFile(MetaInf.pathAg, " ")
  val dfJul = IO.readFile(MetaInf.pathJul, " ")

  //Mudanças no dataframe
  var dfRequest = NasaHttpRequest.UnionJulAug(dfJul, dfAug)
  dfRequest = NasaHttpRequest.RenamedColumn(dfRequest)
  dfRequest = NasaHttpRequest.newColumns(dfRequest)
  dfRequest = NasaHttpRequest.DropColumn(dfRequest)

  Questions.HostNumbers(dfRequest)
  Questions.Erro404(dfRequest)
  Questions.UrlErro(dfRequest)
  Questions.ErroporDia(dfRequest)
  Questions.TotalBytes(dfRequest)

}
