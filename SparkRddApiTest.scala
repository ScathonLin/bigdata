package com.scathon.spark.rdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class RddDemo {
   val conf = new SparkConf().setAppName("rdd").setMaster("local[*]")
   val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")

   @Test
   def glom(): Unit = {
      val a = sc.parallelize(1 to 100, 3)
      println(a.glom().collect.length)
      a.glom.collect.foreach(_.foreach(println))
   }

   @Test
   def getStorageLevel(): Unit = {
      val a = sc.parallelize(1 to 100000, 2)
      a.persist(StorageLevel.DISK_ONLY)
      println(a.getStorageLevel.description)
   }

   @Test
   def preferredLocations(): Unit = {
   }

   @Test
   def getCheckPointFile(): Unit = {
      sc.setCheckpointDir("D:\\checkpoint\\2525e73f-3c82-47bd-9a49-0250691d9905\\rdd-0")
      val a = sc.parallelize(1 to 500, 5)
      val b = a ++ a ++ a ++ a ++ a
      println(b.getCheckpointFile)
   }

   @Test
   def fullOuterJoin(): Unit = {
      val pairRDD1 = sc.parallelize(List(("car", 2), ("car", 5), ("book", 4), ("cat", 12)))
      val pairRDD2 = sc.parallelize(List(("cat", 2), ("cup", 5), ("mouse", 4), ("cat", 12)))
      pairRDD1.fullOuterJoin(pairRDD2).collect.foreach(println)
      println("=================")
      pairRDD1.fullOuterJoin(pairRDD2).collect.foreach(pair => println(pair._1 + "-->" + pair._2))
   }

   @Test
   def foreachPartition(): Unit = {
      val a = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
      //先打印那个分区不一定
      a.foreachPartition(x => println(x.reduce(_ + _)))
   }

   @Test
   def foreach(): Unit = {
      val c = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dplphin", "spider"), 3)
      c.foreach(x => println(x + "s are yummy"))
   }

   @Test
   def foldByKey(): Unit = {
      val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
      val b = a.map(x => (x.length, x))
      b.foldByKey("")(_ + _).collect().foreach(println)
      println("===============")
      val c = sc.parallelize(List("dig", "tiger", "lion", "cat", "panther", "eagle"), 2)
      val d = c.map(x => (x.length, x))
      d.foldByKey("")(_ + _).collect.foreach(println)
   }

   /**
     * 求笛卡尔积
     * (1,6)(1,7)(1,8)...(2,6)(2,7)(2,8)...
     */
   @Test
   def cartesian(): Unit = {
      val x = sc.parallelize(List(1, 2, 3, 4, 5))
      val y = sc.parallelize(List(6, 7, 8, 9, 10))
      printRdd(x.cartesian(y).collect())
   }

   /**
     * 检查点，checkpoint
     * 在下一次计算RDD的时候创建检查点，被Checkpoint的RDD存储在一个二进制文件中，
     * 该文件存在一个可以用SparkContext指定的目录中（注意：Spark提供了懒加载（延迟计算），
     * 检查点知道action动作被触发的时候才会出现），
     */
   @Test
   def checkpoint(): Unit = {
      sc.setCheckpointDir("D:/checkpoint")
      val a = sc.parallelize(1 to 4)
      a.checkpoint()
      println(a.count())
   }

   /**
     * Coalesces the associated data into a given number of partitions.
     * repartition(numPartitions) is simply an abbreviation for coalesce(numPartitions, shuffle = true).
     *
     */
   @Test
   def coalesce(): Unit = {
      val y = sc.parallelize(1 to 10, 10)
      val z = y.coalesce(2, false)
      println(z.partitions.length)
      z.foreach(println)
   }

   /**
     * 功能强大的函数，能够将三个 key-values RDD用Key将他们关联到一起
     */
   @Test
   def cogroup(): Unit = {
      val a = sc.parallelize(List(1, 2, 1, 3), 1)
      val b = a.map((_, "b"))
      printRdd(b.collect())
      val c = a.map((_, "c"))
      printRdd(c.collect())
      //      连接一个
      printRdd(b.cogroup(c).collect())
      val d = a.map((_, "d"))
      //      连接两个
      printRdd(b.cogroup(c, d).collect())
      println("==============")
      val x = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
      val y = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
      printRdd(x.cogroup(y).collect)
      println("==============")
      printRdd(y.cogroup(x).collect)
   }

   /**
     * collect & toArray
     * action operation
     * 将RDD转换成Scala数组并且返回
     */
   @Test
   def collect(): Unit = {
      val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"), 2)
      printRdd(c.collect())
      println("==============")
      val filterFunc = PartialFunction[String, String](ele => {
         ele.toUpperCase()
      })
      c.collect(filterFunc).foreach(println)
      println("===============")
      //      建议使用这种方式，直接collect会导致内存溢出，如果数据量很大
      c.take(10).foreach(println)
   }

   /**
     * 和collect很相似，但是collectAsMap是工作在key-values的RDD，并且将它们转换成Scala的maps
     * 来保持它们的key-value结构
     */
   @Test
   def collectAsMap(): Unit = {
      val a = sc.parallelize(List(1, 2, 1, 3), 1)
      val b = a.zip(a)
      b.collect().foreach(println)
      b.collectAsMap().foreach(println)
   }

   private def printRdd[T](rdd: Array[T]): Unit = {
      rdd.foreach(println)
   }
}
