from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
from pyspark.sql.functions import col

df = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5'),
    ('id_cliente-3',  'cat-6, cat-7'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9'),
    ('id_cliente-9',  'cat-1'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10')
], ['id_cliente', 'categorias'])

start = time.time()

df2 = df.select('id_cliente',explode(split(col("categorias"), ',')).alias("categorias")).withColumn("seqcli", substring('id_cliente',12,3).cast(IntegerType())).withColumn("seqcat", regexp_replace
(col("categorias") , "cat-", "").cast("float").cast(IntegerType())).groupby('id_cliente','seqcli').pivot('seqcat').agg(expr("coalesce(count('seqcat'),0)").alias("cat_")).orderBy("seqcli").na.fill(0).drop("seqcli")
df2.select(*[col(s).alias('cat_'+s) if s != 'id_cliente' else s for s in df2.columns]).show()
print(time.time() - start)


df3 = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3, cat-15'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5, cat-11, cat-14'),
    ('id_cliente-3',  'cat-4, cat-14, cat-15'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10, cat-11'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10, cat-11, cat-13'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9, cat-12, cat-13, cat-14'),
    ('id_cliente-9',  'cat-2'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10')
], ['id_cliente', 'categorias'])

start = time.time()

df2 = df3.select('id_cliente',explode(split(col("categorias"), ',')).alias("categorias")).withColumn("seqcli", substring('id_cliente',12,3).cast(IntegerType())).withColumn("seqcat", regexp_replace
(col("categorias") , "cat-", "").cast("float").cast(IntegerType())).groupby('id_cliente','seqcli').pivot('seqcat').agg(expr("coalesce(count('seqcat'),0)").alias("cat_")).orderBy("seqcli").na.fill(0).drop("seqcli")
df2.select(*[col(s).alias('cat_'+s) if s != 'id_cliente' else s for s in df2.columns]).show()
print(time.time() - start)