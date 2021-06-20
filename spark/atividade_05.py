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

df = spark.sql("SELECT Embreagues, AVG(Idade) AS media_idade FROM si_env GROUP BY Embreagues")

df.coalesce(1).write \
.option("header", "true") \
.format("csv") \
.save("/user/vagrant/atividades/resultados/atividades05")

 
