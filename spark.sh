
spark-shell << EOF

val confidential_parDF = spark.read.parquet("confidential.snappy.parquet")

val nonconfidential_csvDF = spark.read.csv("nonConfidential.csv")

val combineDF = confidential_parDF.union(nonconfidential_csvDF)

val vaDF = combineDF.filter(combineDF("State") === "VA")

vaDF.show(5)

val vacount = vaDF.count()

val ans1 = Seq(("VA",vacount)).toDF("State","count")

ans1.write.csv("tmp/output/q1_Vir")



val ownertypDF = vaDF.groupBy("OwnerTypes").count()

ownertypDF.show()

ownertypDF.write.csv("tmp/output/q2_OwnerType")



combineDF.printSchema()

import org.apache.spark.sql.types._

val vaDF1 = vaDF.withColumn("GrossSqFootTmp",vaDF("GrossSqFoot").cast(IntegerType)).drop("GrossSqFoot").withColumnRenamed("GrossSqFootTmp", "GrossSqFoot")

vaDF1.printSchema()

val totalGrossDF = vaDF1.groupBy("State").sum("GrossSqFoot")

totalGrossDF.show()

totalGrossDF.write.csv("tmp/output/q3_TotalGross")

val cerDF = vaDF1.filter(vaDF1("IsCertified") === "Yes")

cerDF.show(5)

val totalCerGross = cerDF.groupBy("State").sum("GrossSqFoot")

totalCerGross.show()

totalCerGross.write.csv("tmp/output/q3_TotalCerGross")



val countDF = vaDF.groupBy("Zipcode").count()

countDF.show()

val maxCount = countDF.agg(max("count")).show()

val maxZip = countDF.filter(countDF("Count") >= 519)

maxZip.show()

maxZip.write.csv("tmp/output/q4_Max")

EOF
