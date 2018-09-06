#Realizando os import necessarios
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import split, regexp_extract, col, date_format, avg, lit

#Criando SparkContext e sqlContext
sc = SparkContext.getOrCreate()
sqlContext= SQLContext(sc)

#Realizando conexao com o mongodb
uri_mongodb = "mongodb://mongo/geofusion.calculos"
spark = SparkSession.builder.appName("GeoFusionCalculos").config("spark.mongodb.output.uri", uri_mongodb).getOrCreate()

#Caminhos dos arquivos dentro do hdfs
bairros_path = "hdfs:///user/root/bairros.csv"
concorrentes_path = "hdfs:///user/root/concorrentes.csv"
evento_de_fluxo_path = "hdfs:///user/root/eventos_de_fluxo.csv"
populacao_path = "hdfs:///user/root/populacao.json"

#Criando dataframe para cada arquivo
bairros = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimeter",",").load(bairros_path).cache()
concorrentes = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimeter",",").load(concorrentes_path).cache()
evento_de_fluxo = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimeter",",").load(evento_de_fluxo_path).cache()
populacao = spark.read.json(populacao_path).cache()

#pegando o valor da faixa de preco do concorrente
faixa_preco_concorrente = concorrentes.select(['faixa_preco','codigo','nome','endereco'])

#Pegando o valor da populacao por bairro concorrente
populacao_bairro_concorrentes = populacao.join(concorrentes, populacao.codigo == concorrentes.codigo_bairro).select(['populacao',concorrentes.codigo])

#Calculando densidade demografica
densidade_demografica_concorrente = concorrentes.join(bairros, concorrentes.codigo_bairro == bairros.codigo).join(populacao, bairros.codigo == populacao.codigo).select([concorrentes.codigo, bairros.codigo.alias("codigo_bairro"), (bairros.nome).alias('nome_bairro'),bairros.area, populacao.populacao,(populacao.populacao/bairros.area).alias('densidade_demografica')])

#Calculando o fluxo de pessoas por dia da semana
fluxo_dia_semana = evento_de_fluxo.select('codigo_concorrente',date_format('datetime', 'E').alias('weekday'),lit(1).alias('value')).groupBy('codigo_concorrente','weekday').count().select(['codigo_concorrente','weekday',col('count').alias('weekday_count')])

#Calculando o periodo do dia entre manha/tarde/noite
periodo_manha = evento_de_fluxo.select('codigo_concorrente',date_format('datetime', 'E').alias('weekday_manha'),lit(1)).filter((evento_de_fluxo.datetime.substr(12,2) >= 5) & (evento_de_fluxo.datetime.substr(12,2) <= 12)).groupBy('codigo_concorrente','weekday_manha').count().withColumn('periodo_manha', lit('manha')).select(['codigo_concorrente','periodo_manha','weekday_manha',col('count').alias('manha_count')])
periodo_tarde = evento_de_fluxo.select('codigo_concorrente',date_format('datetime', 'E').alias('weekday_tarde'),lit(1)).filter((evento_de_fluxo.datetime.substr(12,2) >= 13) & (evento_de_fluxo.datetime.substr(12,2) <= 18)).groupBy('codigo_concorrente','weekday_tarde').count().withColumn('periodo_tarde', lit('tarde')).select(['codigo_concorrente','periodo_tarde','weekday_tarde',col('count').alias('tarde_count')])
periodo_noite = evento_de_fluxo.select('codigo_concorrente',date_format('datetime', 'E').alias('weekday_noite'),lit(1)).filter((evento_de_fluxo.datetime.substr(12,2) >= 19) & (evento_de_fluxo.datetime.substr(12,2) < 25) | ((evento_de_fluxo.datetime.substr(12,2) >= 0) & (evento_de_fluxo.datetime.substr(12,2) <= 4))).groupBy('codigo_concorrente','weekday_noite').count().withColumn('periodo_noite', lit('noite')).select(['codigo_concorrente','periodo_noite','weekday_noite',col('count').alias('noite_count')])

#Unindo as informacoes coletadas acima
preco_populacao = faixa_preco_concorrente.join(populacao_bairro_concorrentes, faixa_preco_concorrente.codigo == populacao_bairro_concorrentes.codigo).select([faixa_preco_concorrente.codigo,'faixa_preco','populacao','nome','endereco'])
densidade_preco = preco_populacao.join(densidade_demografica_concorrente, preco_populacao.codigo == densidade_demografica_concorrente.codigo).select([preco_populacao.codigo,'nome','endereco','nome_bairro','faixa_preco',preco_populacao.populacao,'densidade_demografica'])
densidade_fluxo = densidade_preco.join(fluxo_dia_semana, densidade_preco.codigo == fluxo_dia_semana.codigo_concorrente).select([densidade_preco.codigo,'nome','endereco','nome_bairro','faixa_preco',densidade_preco.populacao,'densidade_demografica','weekday','weekday_count'])
fluxo_manha = densidade_fluxo.join(periodo_manha, densidade_fluxo.codigo == periodo_manha.codigo_concorrente).filter('weekday == weekday_manha').select([densidade_fluxo.codigo,'nome','endereco','nome_bairro','faixa_preco',densidade_fluxo.populacao,'densidade_demografica','weekday','weekday_count',periodo_manha.periodo_manha,'weekday_manha','manha_count'])
manha_tarde = fluxo_manha.join(periodo_tarde, fluxo_manha.codigo == periodo_tarde.codigo_concorrente).filter('weekday == weekday_tarde').select([fluxo_manha.codigo,'nome','endereco','nome_bairro','faixa_preco',fluxo_manha.populacao,'densidade_demografica','weekday','weekday_count',fluxo_manha.periodo_manha,'manha_count',periodo_tarde.periodo_tarde,'tarde_count'])
join_final = manha_tarde.join(periodo_noite, manha_tarde.codigo == periodo_noite.codigo_concorrente).filter('weekday == weekday_noite').select([manha_tarde.codigo,'nome','endereco','nome_bairro','faixa_preco',manha_tarde.populacao,'densidade_demografica','weekday','weekday_count',manha_tarde.periodo_manha,'manha_count',manha_tarde.periodo_tarde,'tarde_count',periodo_noite.periodo_noite,'noite_count'])

#Inserindo o ultimo dataframe gerado no mongodb
join_final.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
