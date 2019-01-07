package com.avatar.social.cice

/* Ejemplo llamada
  ./bin/spark-submit --class com.avatar.social.cice
  --master local[2] --executor-memory 2G
  --total-executor-cores 2 generateCommonTables/avatarSocial.11-1.0.jar 2018-01-01 */

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext


object generateCommonTables {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Avatar Social Common Tables")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._

    //val vDataDatePart = "2018-01-01" //args(0) cambiar por variable de entrada

    val vDataDatePart =  args(0)

    //PROCESO DE GENERACIÓN DE TABLAS COMUNES


    //SPOTIFY

    val dfSpotify = sqlContext.sql(s"select * from raw_database.table_raw_spotify where data_date_part = '${vDataDatePart}'")

    val dfSpotifyFinal = dfSpotify.withColumn("cantantes",explode ($"spotify.cantantes")).drop($"spotify")

    dfSpotifyFinal.registerTempTable("tSpotify")

    val dfSpotifyTabla = sqlContext.sql("Select id, cantantes.followers, explode(cantantes.genres) as genres, cantantes.name, cantantes.popularity , data_date_part from tSpotify")
    dfSpotifyTabla.registerTempTable("t_spotify")


    val createSQLSpotifyCd = s"create table IF NOT EXISTS common_database.table_spotify partitioned by (data_date_part date) STORED AS TEXTFILE as select id, followers,  genres ,  name,  popularity  from t_spotify where 1 = 0 "
    sqlContext.sql(createSQLSpotifyCd)

    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    //Insert Spotify
    val insertSQLSpotifyCd = s"INSERT OVERWRITE TABLE  common_database.table_spotify PARTITION (DATA_DATE_PART   )  SELECT * FROM t_spotify"

    sqlContext.sql(insertSQLSpotifyCd)


    //FACEBOOK


    val dfFacebook = sqlContext.sql(s"select * from raw_database.table_raw_facebook where data_date_part = '${vDataDatePart}'")

    //Gustos
    val dfFacebookFinalLikes =
      dfFacebook.withColumn("Books_read",explode($"facebook.Books_read"))
        .withColumn("Movies",explode($"facebook.Movies"))
        .withColumn("Movies_watched",explode($"facebook.Movies_watched"))
        .withColumn("Music",explode($"facebook.Music"))
        .withColumn("Other",explode($"facebook.Other"))
        .withColumn("Restaurants",explode($"facebook.Restaurants"))
        .withColumn("Tv_shows_watched",explode($"facebook.Tv_shows_watched"))
        .withColumn("Television",explode($"facebook.Television"))
        .withColumn("Websites",explode($"facebook.Websites")).drop("Facebook")


    dfFacebookFinalLikes.registerTempTable("tFacebook")

    val dfFacebookTablaLikes = sqlContext.sql("Select id , Books_read, Movies, Movies_watched, Music, Other, Restaurants, Tv_shows_watched, Television, Websites, data_date_part from tFacebook")
    dfFacebookTablaLikes.registerTempTable("t_facebook")


    val createSQLFacebookLikesCd = s"create table IF NOT EXISTS common_database.table_facebook_likes partitioned by (data_date_part date) STORED AS TEXTFILE as select id , Books_read, Movies, Movies_watched, Music, Other, Restaurants, Tv_shows_watched, Television, Websites from t_facebook where 1 = 0 "
    sqlContext.sql(createSQLFacebookLikesCd)



    //Insert likes
    val insertSQLFacebookLikesCd = s"INSERT OVERWRITE TABLE  common_database.table_facebook_likes PARTITION (DATA_DATE_PART   )  SELECT *  FROM t_facebook"

    sqlContext.sql(insertSQLFacebookLikesCd)


    //Sesiones


    val dfFacebookTempSessions = dfFacebook.withColumn("sessions",explode ($"facebook.active_sessions")).drop("facebook")

    dfFacebookTempSessions.registerTempTable("facebookTempSessions")
    val dfFacebookFinalSessions =
      sqlContext.sql("""|Select
                        |id,
                        |sessions.app ,
                        |sessions.created_timestamp,
                        |sessions.datr_cookie,
                        |sessions.ip_address,
                        |sessions.location,
                        |sessions.updated_timestamp,
                        |sessions.user_agent,
                        |data_date_part
                        |from facebookTempSessions""".stripMargin)

    dfFacebookFinalSessions.registerTempTable("tFacebookSessions")



    val createSQLFacebookSessionsCd = s"create table IF NOT EXISTS common_database.table_facebook_sessions partitioned by (data_date_part date) STORED AS TEXTFILE as  select id,  app ,  created_timestamp,  datr_cookie, ip_address, location, updated_timestamp, user_agent from tFacebookSessions where 1 = 0 "
    sqlContext.sql(createSQLFacebookSessionsCd)



    //Insert Sessions
    val insertSQLFacebookSessionsCd = s"INSERT OVERWRITE TABLE  common_database.table_facebook_sessions PARTITION (DATA_DATE_PART   )  SELECT *  FROM tFacebookSessions"

    sqlContext.sql(insertSQLFacebookSessionsCd)


    //followers
    val dfFacebookTempFollow= dfFacebook.select($"id",$"facebook.followers",dfFacebook.col("facebook.following").alias("followings"),$"data_date_part")
    dfFacebookTempFollow.registerTempTable("facebookTempFollow")

    val dfFacebookFinalFollow =  sqlContext.sql("Select id, followers, followings , data_date_part from facebookTempFollow")

    dfFacebookFinalFollow.registerTempTable("tFacebookFollow")

    val createSQLFacebookFollowCd=   s"create table IF NOT EXISTS common_database.table_facebook_follow partitioned by (data_date_part date) STORED AS TEXTFILE as  select  id, followers, followings from tFacebookFollow where 1 = 0 "
    sqlContext.sql(createSQLFacebookFollowCd)



    //Insert Follow
    val insertSQLFacebookFollowCd = s"INSERT OVERWRITE TABLE  common_database.table_facebook_follow PARTITION (DATA_DATE_PART   )  SELECT *  FROM tFacebookFollow"

    sqlContext.sql(insertSQLFacebookFollowCd)


    //Interest

    val dfFacebookTempInterests=  dfFacebook.withColumn("ads_interests",explode ($"facebook.ads_interests"))
      .withColumn("event_invitations",explode ($"facebook.event_invitations"))
      .withColumn("page_likes",explode ($"facebook.page_likes"))
      .withColumn("pages_followed",explode ($"facebook.pages_followed")).drop("facebook")


    dfFacebookTempInterests.registerTempTable("dfFacebookInterest")

    val dfFacebookFinalInterests = sqlContext.sql("""Select id, ads_interests,
event_invitations.name as name_invitations, event_invitations.start_timestamp as invitations_start,
event_invitations.end_timestamp as invitations_end, page_likes.name as name_page_likes, page_likes.timestamp as page_likes_timestamp, data_date_part from
dfFacebookInterest""")

    dfFacebookFinalInterests.registerTempTable("tFacebookInterests")


    val createSQLFacebookInterestsCd=   s"create table IF NOT EXISTS common_database.table_facebook_interests partitioned by (data_date_part date) STORED AS TEXTFILE as  select  id, ads_interests, name_invitations, invitations_start, invitations_end, name_page_likes, page_likes_timestamp from tFacebookInterests where 1 = 0 "
    sqlContext.sql(createSQLFacebookInterestsCd)


    //Insert Interests
    val insertSQLFacebookInterestsCd = s"INSERT OVERWRITE TABLE  common_database.table_facebook_interests PARTITION (DATA_DATE_PART   )  SELECT *  FROM tFacebookInterests"
    sqlContext.sql(insertSQLFacebookInterestsCd)


    //profile_info

    val dfProfile = sqlContext.sql(s"select * from raw_database.table_raw_profile where data_date_part = '${vDataDatePart}'")

    val dfprofile_infoFinal = dfProfile.select ($"id",$"profile_info.Country",$"profile_info.city",$"profile_info.email",$"profile_info.name",$"profile_info.surname",$"profile_info.zipcode",$"data_date_part")

    dfprofile_infoFinal.registerTempTable("tProfile_info")

    val createSQLProfileInfo = s"create table IF NOT EXISTS common_database.table_profile_info partitioned by (data_date_part date) STORED AS TEXTFILE as select id, Country, city, email, name,  surname, zipcode from tProfile_info where 1 = 0 "
    sqlContext.sql(createSQLProfileInfo)

    //Insert ProfileInfo
    val insertSQLProfileInfoCd = s"INSERT OVERWRITE TABLE  common_database.table_profile_info PARTITION (DATA_DATE_PART   )  SELECT *  FROM tProfile_info"
    sqlContext.sql(insertSQLProfileInfoCd)


    //Google

    val dfGoogle = sqlContext.sql(s"select * from raw_database.table_raw_google where data_date_part = '${vDataDatePart}'")

    val dfGoogleFinal = dfGoogle.withColumn("resenas",explode ($"google.resenas")).withColumn("sitios_guardados",explode($"google.sitios_guardados")).drop($"google")

    dfGoogleFinal.registerTempTable("tGoogle")

    val dfGoogleTable = sqlContext.sql("""Select id, resenas.Title as resenas_title, resenas.google_url as resenas_google_url, resenas.latitud  as resenas_latitud, resenas.longitud as resenas_longitud,
sitios_guardados.Title as sitios_Title, sitios_guardados.google_url as sitios_url, sitios_guardados.latitud as sitios_latitud, sitios_guardados.longitud as sitios_longitud, data_date_part
from tGoogle """)

    dfGoogleTable.registerTempTable("t_google")


    val createSQLGoogleCd = s"""create table IF NOT EXISTS common_database.table_google partitioned by (data_date_part date) STORED AS TEXTFILE as
 select id, resenas_title,  resenas_google_url,  resenas_latitud,  resenas_longitud, sitios_Title,  sitios_url,  sitios_latitud,   sitios_longitud  from t_google where 1 = 0 """
    sqlContext.sql(createSQLGoogleCd)


    //Insert Google
    val insertSQLGoogleCd = s"INSERT OVERWRITE TABLE  common_database.table_google PARTITION (DATA_DATE_PART   )  SELECT * FROM t_google"

    sqlContext.sql(insertSQLGoogleCd)



  }


}


