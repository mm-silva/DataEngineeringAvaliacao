# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import Row
import matplotlib.pyplot as plt


spark = SparkSession \
    .builder \
    .appName("Total de acidentes com vitima por bairro em acidentes com embriaguez") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE trabalhoFinal")

df = spark.sql("SELECT env.especie_veiculo, bol.desc_tipo_acidente, AVG(env.Idade) AS Idade from " +
                "si_bol_2019 AS bol " +
                "INNER JOIN si_env AS env ON bol.n_boletim = env.num_boletim " +
                "GROUP BY env.especie_veiculo, bol.desc_tipo_acidente")
df.coalesce(1).write \
.option("header", "true") \
.format("csv") \
.save("/user/vagrant/atividades/resultados/atividades_04")

 
