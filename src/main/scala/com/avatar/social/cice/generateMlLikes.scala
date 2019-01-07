package com.avatar.social.cice

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.StringIndexer

object generateMlLikes {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Avatar Social ML Likes")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    //val vDataDatePart = "2018-01-01" //args(0) cambiar por variable de entrada

    val vDataDatePart =  args(0)

    import sqlContext.implicits._

    //GENRES
    val dfGenres = sqlContext.read.table("common_database.table_spotify").select($"id", $"followers",  $"genres" ,  $"name",  $"popularity" ,$"data_date_part").filter($"data_date_part" === vDataDatePart)


    val vWindow = Window.orderBy("id")
    val dfGenresW = dfGenres.withColumn("index", row_number().over(vWindow))

    //Tokenizer
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("genres")
      .setOutputCol("words")
      .setPattern("\\W")

    val tokenized = tokenizer.transform(dfGenresW)


    //stopWordRemover
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")

    val dfTokenized = remover.transform(tokenized)

    dfTokenized.registerTempTable("t_tokenizedGenres")

    val dfFinalGenres = sqlContext.sql("select id, index, explode(filtered) as genresfiltered from t_tokenizedGenres").withColumn("indexGenres", row_number().over(vWindow))

    //StringIndexer
    val indexer = new StringIndexer()
      .setInputCol("genresfiltered")
      .setOutputCol("genresfilteredIndexed")

    val genresIndexed = indexer.fit(dfFinalGenres).transform(dfFinalGenres).orderBy("genresfilteredIndexed")
    //indexed.show()

    val genresFinal = genresIndexed.select($"genresfiltered").distinct.limit(10)


    //DF Likes
    val dfLikes = sqlContext.read.table("common_database.table_facebook_likes").select($"id", $"books_read",  $"movies" ,  $"television",$"data_date_part").filter($"data_date_part" === vDataDatePart)

    //Books

    val dfBooks = dfLikes.select("books_read").distinct

    val vBooksWindow = Window.orderBy("books_read")
    val dfBooksW = dfBooks.withColumn("books_read_id", row_number().over(vBooksWindow))

    //StringIndexer
    val indexerBook = new StringIndexer()
      .setInputCol("books_read")
      .setOutputCol("books_read_index")

    val bookIndexed = indexerBook.fit(dfBooksW).transform(dfBooksW).orderBy("books_read_index")
    //indexed.show()

    val booksFinal = bookIndexed.select($"books_read").distinct.limit(10)

  //Movies

    val dfMovies = dfLikes.select("movies").distinct

    val vMoviesWindow = Window.orderBy("movies")
    val dfMoviesW = dfMovies.withColumn("movies_id", row_number().over(vMoviesWindow))

    //StringIndexer
    val indexerMovies = new StringIndexer()
      .setInputCol("movies")
      .setOutputCol("movies_index")

    val moviesIndexed = indexerMovies.fit(dfMoviesW).transform(dfMoviesW).orderBy("movies_index")
    //indexed.show()

    val moviesFinal = moviesIndexed.select($"movies").distinct.limit(10)



    //name_artist_spotify



    val dfArtist = dfGenres.select("name").distinct

    val vArtistWindow = Window.orderBy("name")
    val dfArtistW = dfArtist.withColumn("artist_id", row_number().over(vArtistWindow))

    //StringIndexer
    val indexerArtist = new StringIndexer()
      .setInputCol("name")
      .setOutputCol("artist_index")

    val artistIndexed = indexerArtist.fit(dfArtistW).transform(dfArtistW).orderBy("artist_index")
    //indexed.show()

    val artistFinal = artistIndexed.select($"name").distinct.limit(10)

    //Generate table bu_LIKES

    val artistList = artistFinal.map(x => x.getString(0)).collect.toList



    val moviesList = moviesFinal.map(x => x.getString(0)).collect.toList
    val booksList = booksFinal.map(x => x.getString(0)).collect.toList
    val genresList = genresFinal.map(x => x.getString(0)).collect.toList



    val tempList = genresList ++ moviesList ++ artistList ++ booksList
    val finalList = tempList.map(x => x.replace (" ","_"))


    val vCampos = finalList.foldRight(List[String]())((x, acc) => x  + " " + "STRING" + "," :: acc).mkString(" ")



    val createFinal =  "create table IF NOT EXISTS business_database.table_users_catg_likes_"+  vDataDatePart.replace("-","") +" (ID STRING, " + vCampos + "  DATA_DATE_PART STRING) "

    sqlContext.sql(createFinal)


    val tableFinal = sqlContext.table(s"business_database.table_users_catg_likes_"+  vDataDatePart.replace("-",""))

    val dfFinal = tableFinal.drop(tableFinal.col("id") ).drop(tableFinal.col("data_date_part"))


  }

}
