from pyspark.sql.functions import col
start = time.time()
df2 = df.select('id_cliente',explode(split(col("categorias"), ',')).alias("categorias")).withColumn("seqcli", substring('id_cliente',12,3).cast(IntegerType())).withColumn("seqcat", regexp_replace
(col("categorias") , "cat-", "").cast("float").cast(IntegerType())).groupby('id_cliente','seqcli').pivot('seqcat').agg(expr("coalesce(count('seqcat'),0)").alias("cat_")).orderBy("seqcli").na.fill(0).drop("seqcli")
df2.select(*[col(s).alias('cat_'+s) if s != 'id_cliente' else s for s in df2.columns]).show()
print(time.time() - start)