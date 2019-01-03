package com.avatar.json

/* Ejemplo llamada
  ./bin/spark-submit --class com.avatar.json.extractJson
  --master local[2] --executor-memory 2G
  --total-executor-cores 2 extractJson/avatarSocial.11-1.0.jar File:///usr/pruebas/apps_and_websites_sin_n.json */

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object extractJson {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val fileName = args(0)

    //val prueba = sqlContext.read.json("File:///usr/pruebas/apps_and_websites_sin_n.json")

    val jsonInstalled = sqlContext.read.json(fileName)
    val dfInstalled = jsonInstalled.select(explode(jsonInstalled("installed_apps"))).toDF("installed_apps")

    val dfInstalledFinal = dfInstalled.select("installed_apps.name","installed_apps.added_timestamp")

    dfInstalledFinal.write.saveAsTable("prueba")

  }
}


