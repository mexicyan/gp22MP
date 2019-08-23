package com.Tags

import java.util

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsAppName extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()



    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取appid，和appname
    val appid: String = row.getAs[String]("appid")

    val appname: String = row.getAs[String]("appname")




    val appMap: Map[String, String] = args(1).asInstanceOf[Map[String,String]]



    val jedis = new Jedis("hadoop02", 6379)
    val appinfo : util.Map[String, String]  = jedis.hgetAll("AppInfo")



//    val Appinfores =  if (appname.equals("其他")) appMap.getOrElse(appid,"未知") else appname

    val Appinfores =  if (appname.equals("其他")) appinfo.getOrDefault(appid,"未知") else appname

    if(StringUtils.isNotBlank(Appinfores)){
      list:+=("LN"+Appinfores,1)
    }

    jedis.close()

    list
  }

}
