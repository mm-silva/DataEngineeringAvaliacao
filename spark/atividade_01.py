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

df = spark.sql("SELECT log.nome_bairro, count(*) AS num_vitimas FROM " +
                "si_env AS env " +
                "INNER JOIN si_log_2019 AS log " + 
                "ON env.num_boletim = log.n_boletim WHERE env.Embreagues = 'SIM'" +
                "GROUP BY log.nome_bairro")


df.coalesce(1).write \
.option("header", "true") \
.format("csv") \
.save("/user/vagrant/atividades/resultados/atividades01")

 
