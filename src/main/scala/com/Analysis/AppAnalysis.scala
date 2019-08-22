package com.Analysis

import java.io

import com.utils.RptUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object AppAnalysis {
  def main(args: Array[String]): Unit = {
    // 创建一个集合保存输入和输出目录
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    val df = sQLContext.read.parquet("dir/resource/*")
    val appinfo = sQLContext.read.text("dir/dict/*")
        .map(row => {
          val appinfo: Array[String] = row.toString().split("\t",-1).filter(_.length>4)
          (appinfo(4),appinfo(1))
        })

    val map: Map[String, String] = appinfo.collect().toMap

    // 将数据进行处理，统计各个指标
    val logInfo: RDD[(String, String, List[Double])] = df.map(row => {
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")


      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
      // 创建三个对应的方法处理九个指标

      val requlist = RptUtils.request(requestmode, processnode)
      val clicklist = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment).map(x => {
        List[Double](x(0), x(1), x(2) * 1000, x(3) * 1000)
      })


      (appid, appname, requlist ++ clicklist ++ adlist)


    })
    val Appinfores = logInfo.map(arr => (if (arr._1.equals("其他")) map.getOrElse(arr._1, "未知") else arr._2, arr._3))
    //先判定appname是否为空，若为空则用appid去字典找
    Appinfores
      .reduceByKey((list1,list2) =>{
        list1.zip(list2).map(t => t._1+t._2)
      })
      .map(t => {
        t._1+","+t._2.mkString(",")
      }).saveAsTextFile("dir/out/App")


  }
}
