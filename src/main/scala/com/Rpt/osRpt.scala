package com.Rpt

import com.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object osRpt {
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
    // 将数据进行处理，统计各个指标
    df.map(row=>{
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
      // key 值  是地域的省市
      val client = row.getAs[String]("client")

      var clientos = ""
      if(client == 1){
        clientos = "android"
      }else if(client == 2){
        clientos = "ios"
      }else{
        clientos = "其他"
      }
      // 创建三个对应的方法处理九个指标

      val requlist = RptUtils.request(requestmode,processnode)
      val clicklist = RptUtils.click(requestmode,iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment).map(x=>{
        List[Double](x(0),x(1),x(2)*1000,x(3)*1000)
      })


      ((clientos),requlist++clicklist++adlist)
    }).reduceByKey((list1,list2) =>{
      list1.zip(list2).map(t => t._1+t._2)
    })
      .map(t => {
        t._1+","+t._2.mkString(",")
      }).saveAsTextFile("dir/out/OS")
  }
}
