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

df = spark.sql("SELECT desc_tempo, pavimento, COUNT(*) AS num_vitimas FROM " +
                "si_bol_2019 GROUP BY desc_tempo, pavimento")

df.coalesce(1).write \
.option("header", "true") \
.format("csv") \
.save("/user/vagrant/atividades/resultados/atividades02")

 
