package com.Tags

import com.utils.TagUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 4){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dirPath,stopPath)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    // 读取字段文件
    val map = sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    // 将处理好的数据广播
    val broadcast = sc.broadcast(map)

    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 过滤符合Id的数据
   val res = df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .map(row => {
      // 取出用户Id
      val userId = TagUtils.getOneUserId(row)
      // 接下来的通过Row数据，打上所有标签（按照需求）
      val adtagslist: List[(String, Int)] = TagsAd.makeTags(row)

      val appnamelist: List[(String, Int)] = TagsAppName.makeTags(row,broadcast.value)

      val adplatformlist: List[(String, Int)] = TagsChannel.makeTags(row)

      val machinetagslist: List[(String, Int)] = TagsMachine.makeTags(row)

      val keywordList = TagsKeyWords.makeTags(row,bcstopword)

      val locationtagslist: List[(String, Int)] = TagsLocation.makeTags(row)

      (userId,adtagslist++appnamelist++adplatformlist++machinetagslist++keywordList++locationtagslist)
    })
    res.foreach(println)
  }
}
