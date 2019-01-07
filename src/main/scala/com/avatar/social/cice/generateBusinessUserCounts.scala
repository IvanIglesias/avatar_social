package com.avatar.social.cice


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object generateBusinessUserCounts {


   def main(args: Array[String]) {

      val conf = new SparkConf().setAppName("Avatar Social User Counts")
      val sc = new SparkContext(conf)
      val sqlContext = new HiveContext(sc)

     sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

      import sqlContext.implicits._

     val vDataDatePart =  args(0)
     // val vDataDatePart = "2018-01-01" //cambiar por variable de entrada

      val dfFollow = sqlContext.read.table("common_database.table_facebook_follow").select($"id",$"followers",$"followings",$"data_date_part").filter($"data_date_part" === vDataDatePart)
      val dfGoogle = sqlContext.read.table("common_database.table_google").select($"id",$"resenas_title",$"sitios_title",$"data_date_part").filter($"data_date_part" === vDataDatePart)

      val dfResenasCount = dfGoogle.groupBy(dfGoogle.col("id")).agg(countDistinct("resenas_title").alias("count_resenas"))
      val dfResenasSitiosCount = dfGoogle.groupBy(dfGoogle.col("id")).agg(countDistinct("sitios_title").alias("count_sitios"))

      val dfGroupTable = dfFollow.join(dfResenasCount, dfFollow.col("id") === dfResenasCount.col("id"),"left").join(dfResenasSitiosCount, dfFollow.col("id") === dfResenasSitiosCount.col("id"),"left").select(dfFollow.col("id"),$"followers",$"followings",$"count_resenas",$"count_sitios",dfFollow.col("data_date_part")).distinct

      //más funcionalidad a la tabla
      val dfGroupClass = dfGroupTable.withColumn("sumValues", expr("followers + followings + count_resenas + count_sitios"))

      dfGroupClass.registerTempTable("t_GroupTable")

      val createSQLGroupUserCountBu = s"""create table IF NOT EXISTS business_database.table_User_Counts partitioned by (data_date_part date) STORED AS TEXTFILE as
 	    select id, followers,  followings,  count_resenas,  count_sitios , sumValues from t_GroupTable where 1 = 0 """
      sqlContext.sql(createSQLGroupUserCountBu)

      //Insert Google
      val insertGroupUserCountBu = s"INSERT OVERWRITE TABLE  business_database.table_User_Counts PARTITION (DATA_DATE_PART   )  SELECT id, followers,  followings,  count_resenas,  count_sitios , sumValues FROM t_GroupTable"

      sqlContext.sql(insertGroupUserCountBu)

  }


}
