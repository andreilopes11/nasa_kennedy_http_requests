# nasa_http_requests

Fonte​ ​ oficial​ ​ do​ ​ dateset​ : ​ ​ http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

Dados​ :

●  ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz

●  ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz


Sobre o dataset​ : Esses dois conjuntos de dados possuem todas as requisições HTTP para o servidor da NASA Kennedy
Space​ ​ Center​ ​ WWW​ ​ na​ ​ Flórida​ ​ para​ ​ um​ ​ período​ ​ específico.
Os​ ​ logs​ ​ estão​ ​ em​ ​ arquivos​ ​ ASCII​ ​ com​ ​ uma​ ​ linha​ ​ por​ ​ requisição​ ​ com​ ​ as​ ​ seguintes​ ​ colunas:

● Host fazendo a requisição​ . Um hostname quando possível, caso contrário o endereço de internet se o nome
não​ ​ puder​ ​ ser​ ​ identificado.

● Timestamp​ ​ no​ ​ formato​ ​ "DIA/MÊS/ANO:HH:MM:SS​ ​ TIMEZONE"

● Requisição​ ​ (entre​ ​ aspas)

● Código​ ​ do​ ​ retorno​ ​ HTTP

● Total​ ​ de​ ​ bytes​ ​ retornados

# Questões
​
Responda​ ​ as​ ​ seguintes​ ​ questões​ ​ devem​ ​ ser​ ​ desenvolvidas​ ​ em​ ​ Spark​ ​ utilizando​ ​ a ​ ​ sua​ ​ linguagem​ ​ de​ ​ preferência.
1. Número​ ​ de​ ​ hosts​ ​ únicos
2. O​ ​ total​ ​ de​ ​ erros​ ​ 404
3. Os​ ​ 5 ​ ​ URLs​ ​ que​ ​ mais​ ​ causaram​ ​ erro​ ​ 404
4. Quantidade​ ​ de​ ​ erros​ ​ 404​ ​ por​ ​ dia.
5. O​ ​ total​ ​ de​ ​ bytes​ ​ retornados.

-Dspark.master=local[*]
