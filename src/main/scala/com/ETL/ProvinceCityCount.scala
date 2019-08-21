package com.ETL

import com.Utils.Utils2Type
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

object ProvinceCityCount {
  def main(args: Array[String]): RDD[Row] = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("locak[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val sql = new SQLContext(sc)

    sql.setConf("spark.sql.parquet.compression.codec","snappy")

    val lines = sc.textFile("dir/")

    val RddAll = getAll(lines,",")

    val provinceAndCity: RDD[Row] = RddAll.map(x => {
      val province: String = x.toString(24)
      val city: String = x.toString(25)
      Row(province, city)
    })



    val structtypeProvinceAndCity = StructType(
      Seq(
        StructField("provincename",StringType),
        StructField("cityname",StringType)
      )
    )

    val df: DataFrame = sql.createDataFrame(provinceAndCity, structtypeProvinceAndCity)














  }
  def getAll(rdd: RDD[String],s:String): RDD[Row] ={
    val res: RDD[Row] = rdd.map(t => t.split(",", t.length)).filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          Utils2Type.toInt(arr(1)),
          Utils2Type.toInt(arr(2)),
          Utils2Type.toInt(arr(3)),
          Utils2Type.toInt(arr(4)),
          arr(5),
          arr(6),
          Utils2Type.toInt(arr(7)),
          Utils2Type.toInt(arr(8)),
          Utils2Type.toDouble(arr(9)),
          Utils2Type.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          Utils2Type.toInt(arr(17)),
          arr(18),
          arr(19),
          Utils2Type.toInt(arr(20)),
          Utils2Type.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          Utils2Type.toInt(arr(26)),
          arr(27),
          Utils2Type.toInt(arr(28)),
          arr(29),
          Utils2Type.toInt(arr(30)),
          Utils2Type.toInt(arr(31)),
          Utils2Type.toInt(arr(32)),
          arr(33),
          Utils2Type.toInt(arr(34)),
          Utils2Type.toInt(arr(35)),
          Utils2Type.toInt(arr(36)),
          arr(37),
          Utils2Type.toInt(arr(38)),
          Utils2Type.toInt(arr(39)),
          Utils2Type.toDouble(arr(40)),
          Utils2Type.toDouble(arr(41)),
          Utils2Type.toInt(arr(42)),
          arr(43),
          Utils2Type.toDouble(arr(44)),
          Utils2Type.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          Utils2Type.toInt(arr(57)),
          Utils2Type.toDouble(arr(58)),
          Utils2Type.toInt(arr(59)),
          Utils2Type.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(73),
          Utils2Type.toInt(arr(76)),
          Utils2Type.toDouble(arr(77)),
          Utils2Type.toDouble(arr(78)),
          Utils2Type.toDouble(arr(79)),
          Utils2Type.toDouble(arr(80)),
          Utils2Type.toDouble(arr(81)),
          arr(82),
          arr(83),
          arr(84),
          arr(85),
          arr(86),
          Utils2Type.toInt(arr(87))
        )
      })
    res
  }

}
