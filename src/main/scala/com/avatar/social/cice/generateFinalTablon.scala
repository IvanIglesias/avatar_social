package com.avatar.social.cice

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object generateFinalTablon {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Avatar Social User Counts")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    import sqlContext.implicits._

    //val vDataDatePart = args(0)

    val dfTablonFinal =  sqlContext.sql("""|select  a.id as user_id,
                      |a.name as user_name,
                      |a.surname as user_surname,
                      |a.country as user_country,
                      |a.city as user_city,
                      |a.zipcode as user_zip_code,
                      |a.followers as followers,
                      |a.followings as followings,
                      |count(distinct a.books_read) as book_read,
                      |count(distinct a.movies) as movies,
                      |count(distinct a.television) as television,
                      |count(distinct a.artist) as artists_names,
                      |count(distinct a.location) as facebook_location,
                      |count(distinct a.resenas_title)  as reviews,
                      |count(distinct a.sitios_title) as sites,
                      |max(a.data_date_part) as actual_date
                      |from
                      |(select profile.id,
                      |profile.name,
                      |profile.surname,
                      |profile.country,
                      |profile.city,
                      |profile.zipcode,
                      |profile.data_date_part,
                      |follow.followers,
                      |follow.followings,
                      |likes.books_read,
                      |likes.movies,
                      |likes.television,
                      |spotify.name artist,
                      |fa_sessions.location,
                      |google.resenas_title,
                      |google.sitios_title
                      |from common_database.table_profile_info profile
                      |left join common_database.table_facebook_follow follow
                      |on profile.id = follow.id
                      |and profile.data_date_part = follow.data_date_part
                      |left join common_database.table_facebook_likes likes
                      |on profile.id = likes.id
                      |and profile.data_date_part = likes.data_date_part
                      |left join common_database.table_spotify spotify
                      |on profile.id = spotify.id
                      |and profile.data_date_part = spotify.data_date_part
                      |left join common_database.table_facebook_sessions fa_sessions
                      |on profile.id = fa_sessions.id
                      |and profile.data_date_part = fa_sessions.data_date_part
                      |left join common_database.table_google google
                      |on profile.id = google.id
                      |and profile.data_date_part = google.data_date_part
                      |)a
                      |group by
                      |a.id,
                      |a.name,
                      |a.surname,
                      |a.country,
                      |a.city,
                      |a.zipcode,
                      |a.followers,
                      |a.followings
                      """.stripMargin)


    dfTablonFinal.registerTempTable("t_tablon")


    val createSQLtablonBu = s"""create table IF NOT EXISTS business_database.table_user_live  STORED AS TEXTFILE as select user_id, user_name, user_surname, user_country,  user_city,  user_zip_code,  followers, followings,  book_read, movies,  television, artists_names,  facebook_location, reviews,  sites, actual_date from t_tablon where 1 = 0 """
    sqlContext.sql(createSQLtablonBu)


    //Insert Google
    val insertSQLtablonBu = s"INSERT OVERWRITE TABLE  business_database.table_user_live  SELECT * FROM t_tablon"

    sqlContext.sql(insertSQLtablonBu)


  }

}


