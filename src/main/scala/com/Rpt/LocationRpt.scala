package com.Rpt

import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.util.Properties

import com.typesafe.config._
import com.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 地域分布指标
  */
object LocationRpt {
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
    val localRes: RDD[((String, String), List[Double])] = df.map(row => {
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
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标

      val requlist = RptUtils.request(requestmode, processnode)
      val clicklist = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)


      ((pro, city), requlist ++ clicklist ++ adlist)
    })
    localRes
      .reduceByKey((list1,list2) =>{
      list1.zip(list2).map(t => t._1+t._2)
    })
      .map(t => {
        t._1+","+t._2.mkString(",")
      }).saveAsTextFile("dir/out/Localtion")


      localRes.foreachPartition(data2MySQL)





  }







  def getProperties() = {
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    prop.setProperty("Driver",load.getString("jdbc.driver"))
    (prop, load.getString("jdbc.url"))
  }

  //尝试使用连接池








  //之前学过的用法

  val data2MySQL = (it: Iterator[((String, String),List[Double])]) => {
    var conn: Connection = null;
    var ps: PreparedStatement = null;

    val sql = "insert into location_info(province, city, originalReques,validRequest，successRequest，participateBid，ParticiateSuccessed，show,click,winprice,adpayment) values(?,?,?,?,?,?,?,?,?,?,?)"

    val jdbcUrl = "jdbc:mysql://haodoop02:3306/exam?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "root"

    try {
      conn = DriverManager.getConnection(jdbcUrl, user, password)
      it.foreach(tup => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, tup._1._1)
        ps.setString(2, tup._1._2)
        ps.setDouble(3, tup._2(0))
        ps.setDouble(4, tup._2(1))
        ps.setDouble(5, tup._2(2))
        ps.setDouble(6, tup._2(5))
        ps.setDouble(7, tup._2(6))
        ps.setDouble(8, tup._2(3))
        ps.setDouble(9, tup._2(4))
        ps.setDouble(10, tup._2(7))
        ps.setDouble(11, tup._2(8))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }
}
