package com.avatar.social.cice

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext


/* Ejemplo llamada
  ./bin/spark-submit --class com.avatar.social.cice
  --master local[2] --executor-memory 2G
  --total-executor-cores 2 generateRawTables/avatarSocial.11-1.0.jar File:///home/cloudera/Downloads/Final_02_03_2019.json 2018-01-01 */


object generateRawTables {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Avatar Social generate Raw Tables")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._

  //  val txtJson = sqlContext.read.json("File:///home/cloudera/Downloads/Final_02_03_2019.json")    // val fileName = args(0)
    //val txtJson = sqlContext.read.json("File:///home/cloudera/Downloads/Final_07_03_2019.json")
    val fileName = args(0)
    val txtJson = sqlContext.read.json(fileName)
    //val vDataDatePart = "2018-01-01" //cambiar por variable de entrada val fileName = args(1)
    //val vDataDatePart = "2018-01-02"

    val vDataDatePart =  args(1)
    //Creación de tablas RAW
    val dfRawFacebook = txtJson.select($"user_id.id", $"facebook",$"date_part.data_date_part")
    val dfRawGoogle = txtJson.select($"user_id.id",$"google",$"date_part.data_date_part")
    val dfRawSpotify = txtJson.select($"user_id.id",$"spotify",$"date_part.data_date_part")
    val dfRawProfile = txtJson.select($"user_id.id",$"profile_info",$"date_part.data_date_part")

    //Registro tablas temporales RAW
    dfRawFacebook.registerTempTable("table_raw_facebook")
    dfRawGoogle.registerTempTable("table_raw_google")
    dfRawSpotify.registerTempTable("table_raw_spotify")
    dfRawProfile.registerTempTable("table_raw_profile")


    val createSQLFacebook = s"create table IF NOT EXISTS raw_database.table_raw_facebook partitioned by (data_date_part date) STORED AS TEXTFILE as select id,facebook from table_raw_facebook where 1 = 0 "
    sqlContext.sql(createSQLFacebook)

    val createSQLGoogle = s"create table IF NOT EXISTS raw_database.table_raw_google partitioned by (data_date_part date) STORED AS TEXTFILE as select id, google from table_raw_google where 1 = 0 "
    sqlContext.sql(createSQLGoogle)

    val createSQLSpotify = s"create table IF NOT EXISTS raw_database.table_raw_spotify partitioned by (data_date_part date) STORED AS TEXTFILE as select id, spotify from table_raw_spotify where 1 = 0 "
    sqlContext.sql(createSQLSpotify)

    val createSQLProfile = s"create table IF NOT EXISTS raw_database.table_raw_profile partitioned by (data_date_part date) STORED AS TEXTFILE as select id, profile_info from table_raw_profile where 1 = 0 "
    sqlContext.sql(createSQLProfile)

    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    //Insert Facebook
    val insertSQLFacebook = s"""INSERT OVERWRITE TABLE  raw_database.table_raw_facebook PARTITION (DATA_DATE_PART)  SELECT *  from table_raw_facebook"""
    sqlContext.sql(insertSQLFacebook)
    //Insert Google
    val insertSQLGoogle = s"INSERT OVERWRITE TABLE  raw_database.table_raw_google PARTITION (DATA_DATE_PART   )  SELECT *  FROM table_raw_google"
    sqlContext.sql(insertSQLGoogle)
    //Insert Spotify
    val insertSQLSpotify = s"INSERT OVERWRITE TABLE  raw_database.table_raw_spotify PARTITION (DATA_DATE_PART   )  SELECT *  FROM table_raw_spotify"
    sqlContext.sql(insertSQLSpotify)
    //Insert Profile
    val insertSQLProfile = s"INSERT OVERWRITE TABLE  raw_database.table_raw_profile PARTITION (DATA_DATE_PART   )  SELECT *  FROM table_raw_profile"
    sqlContext.sql(insertSQLProfile)

  }


}
