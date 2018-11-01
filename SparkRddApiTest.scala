package com.scathon.spark.rdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class RddDemo {
   val conf = new SparkConf().setAppName("rdd").setMaster("local[*]")
   val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")


   @Test
   def randomSplit(): Unit = {
      val y = sc.parallelize(1 to 10)
      val splits = y.randomSplit(Array(0.6, 0.4), seed = 11L)
      val training = splits(0)
      val test = splits(1)
      training.collect
   }

   /**
     * 从分区中取出RDD数据，然后将数据通过标准输入发给shell命令。
     * 命令结果输出被捕获，并且返回一系列字符串值的RDD
     */
   @Test
   def pipe(): Unit = {
      val a = sc.parallelize(1 to 9, 3)
      a.pipe("head -n 1").collect
   }

   /**
     * def cache():RDD[T],
     * def persist():RDD[T]
     * def persist(newLevel:StorageLevel):RDD[T]
     * 这些函数可以用来调整RDD的存储级别，当释放内存的时候，Spark将会根据存储
     * 级别来判定分区该如何保存。cache(),persist()两个函数仅仅是persist(StorageLevel.MEMORY_ONLY)，
     * 也就是基于内存存储，注意，一旦存储级别被设置了，无法再进行修改。
     */
   @Test
   def persist_cache(): Unit = {
      val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
      println(c.getStorageLevel)
      println(c.cache())
      val d = c.persist(StorageLevel.MEMORY_AND_DISK_2)
      println(d.getStorageLevel)
      println(d.cache())
   }

   @Test
   def partitionBy(): Unit = {
      val a = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 3)
      a.partitions.foreach(partition => println(partition.index))
   }

   /**
     * 给一个RDD标识一个名字
     */
   @Test
   def nameOrSetName(): Unit = {
      val y = sc.parallelize(1 to 10, 3)
      println(y.name)
      y.setName("Scathon Lin RDD")
      println(y.name)
   }

   /**
     * 调用stats并提取平均值组件。在某些情况下，函数的近似版本可以更快地完成。然而，它用准确性换取速度。
     */
   @Test
   def mean(): Unit = {
      val a = sc.parallelize(List(9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5), 3)
      println(a.mean)
   }

   @Test
   def max(): Unit = {
      val y = sc.parallelize(10 to 30)
      println(y.max)
      val a = sc.parallelize(List((10, "dog"), (3, "tiger"), (9, "lion"), (18, "cat")))
      // 找出的是key最大的那个tuple
      println(a.max)
   }

   @Test
   def mapWith(): Unit = {
      //Deprecated
   }

   /**
     * 抽取包含两个元素的元组的RDD，然后应用给出的转化函数，将每一个元素进行转换，生成新的值，
     * 然后将key和经过转换的新的value
     * 构建新的tuple，存储到新的RDD中返回。
     */
   @Test
   def mapValues(): Unit = {
      val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
      val b = a.map(x => (x.length, x))
      /*
        * (3,xdogx)
        * (5,xtigerx)
        * (4,xlionx)
        * (3,xcatx)
        * (7,xpantherx)
        * (5,xeaglex)
        */
      b.mapValues("x" + _ + "x").collect.foreach(println)
   }

   @Test
   def mapPartitionsWithSplit(): Unit = {
      //被遗弃的API
   }

   // 和mapPartitions类似，但是需要两个参数，第一个参数，是partition的索引标号，第二个参数是整个分区内的所有元素的迭代器
   // 输出结果是一个包含应用了transformation函数之后的所有元素的迭代器，
   @Test
   def mapPartitionsWithIndex(): Unit = {
      val x = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

      def myfunc(index: Int, iter: Iterator[Int]): Iterator[String] = {
         iter.map(x => index + "," + x)
      }

      x.mapPartitionsWithIndex(myfunc).collect
      //Spark Shell 输出结果：
      //res9: Array[String] = Array(0,1, 0,2, 0,3, 1,4, 1,5, 1,6, 2,7, 2,8, 2,9, 2,10)
   }

   @Test
   def mapPartitions_E3(): Unit = {
      val x = sc.parallelize(1 to 10, 3)
      x.flatMap(List.fill(scala.util.Random.nextInt(10))(_)).collect.foreach(println)
      // Spark Shell 运行结果
      // res8: Array[Int] = Array(1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 8, 9, 9, 9, 9, 9, 9, 10)
   }

   @Test
   def mapPartitions_E2(): Unit = {
      val x = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

      def myfunc(iter: Iterator[Int]): Iterator[Int] = {
         var res = List[Int]()
         while (iter.hasNext) {
            val cur = iter.next;
            // List.fill(m)(n) 作用是，生成一个有m个元素，元素值是n的list
            res = res ::: List.fill(scala.util.Random.nextInt(10))(cur)
         }
         res.iterator
      }
      //Spark Shell 运行结果：
      //res7: Array[Int] = Array(1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2,
      // 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7,
      // 8, 8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 10, 10, 10, 10, 10, 10)
      x.mapPartitions(myfunc).collect
   }

   @Test
   def mapPartitions_E1(): Unit = {
      val a = sc.parallelize(1 to 9, 3)

      def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
         var res = List[(T, T)]()
         var pre = iter.next
         while (iter.hasNext) {
            val cur = iter.next
            res.::=(pre, cur)
            pre = cur
         }
         res.iterator
      }

      //  这一句本地运行不起来，spark-shell中运行结果：
      //  res3: Array[(Int, Int)] = Array((2,3), (1,2), (5,6), (4,5), (8,9), (7,8))
      a.mapPartitions(myfunc).collect.foreach(println)
   }

   /**
     * 应用transformation函数到RDD中的每个元素，并且返回一个新的RDD。
     */
   @Test
   def map(): Unit = {
      val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
      val b = a.map(_.length)
      val c = a.zip(b)
      c.collect.foreach(println)
   }

   /**
     * 扫描与给出的值相同的keys，并且将他们作为scala sequence返回。
     */
   @Test
   def lookup(): Unit = {
      val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
      val b = a.map(x => (x.length, x))
      b.lookup(5).foreach(println)
   }

   /**
     * 使用两个key-values的RDDs进行左外连接，明确一点就是，keys必须是可以准确进行比较的。
     */
   @Test
   def leftOuterJoin(): Unit = {
      val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
      //(3,"dog),(6,"salmon")...
      val b = a.keyBy(_.length)
      b.foreach(println)
      println("============================")
      val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
      //(3,"dog"),(3,"cat"),(6,"rabbit")
      val d = c.keyBy(_.length)
      d.foreach(println)
      println("============================")
      println(b.leftOuterJoin(d).collect)
      b.leftOuterJoin(d).collect.foreach(println)
   }

   /**
     * 抽取元组中的所有的keys，然后返回一个新的RDD
     */
   @Test
   def keys(): Unit = {
      val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
      val b = a.map(x => (x.length, x))
      println(b.keys.collect)
      b.keys.collect.foreach(println)
      b.values.collect.foreach(println)
   }

   /**
     * 生成键值对，传入一个生成key的函数，然后生成对应的(key,value)元组
     */
   @Test
   def keyBy(): Unit = {
      val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
      val b = a.keyBy(_.length)
      println(b.collect) // 元组数组
      b.collect.foreach(println)
      val c = a.keyBy(_.toUpperCase())
      c.collect.foreach(println)
   }

   @Test
   def join(): Unit = {
      val a = sc.parallelize(List("dog", "salmon", "rat", "elephant"), 3)
      // (3,"dog")(6,"salmon")...
      val b = a.keyBy(_.length)
      val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
      // (3,"dog")(6,"salmon")...
      val d = c.keyBy(_.length)
      b.join(d).collect().foreach(println)
   }

   @Test
   def iterator(): Unit = {
      // 返回此RDD分区的兼容迭代器对象。永远不要直接调用这个函数。
   }

   @Test
   def isCheckpointed(): Unit = {
      val c = sc.parallelize(1 to 10, 2)
      sc.setCheckpointDir("D:/checkpoints/test_checkpoints")
      println(c.isCheckpointed)
      c.checkpoint()
      println(c.isCheckpointed)
      println(c.collect.foreach(print))
      println(c.isCheckpointed)
   }

   @Test
   def intersection(): Unit = {
      val x = sc.parallelize(1 to 25)
      val y = sc.parallelize(20 to 30)
      val z = x.intersection(y)
      z.collect.foreach(println)
   }

   //*
   @Test
   def id(): Unit = {
      val y = sc.parallelize(1 to 10, 10)
      println(y.id)
   }

   // **
   @Test
   def histogram(): Unit = {
      val a = sc.parallelize(List(1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0), 3)
      a.histogram(5)
   }

   @Test
   def groupByKey(): Unit = {
      val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
      // 传入一个生成key的函数，然后生成一个tuple=>(key generated by func,value)
      // (3,dog)
      val b = a.keyBy(_.length)
      b.collect.foreach(println)
      b.groupByKey.collect.foreach(println)
   }

   //=============================
   import org.apache.spark.Partitioner

   class MyPartitioner extends Partitioner {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = {
         key match {
            case null => 0
            case key: Int => key
            case _ => key.hashCode() % numPartitions
         }
      }

      override def equals(other: scala.Any): Boolean = {
         other match {
            case h: MyPartitioner => true
            case _ => false
         }
      }
   }

   @Test
   def groupBy02(): Unit = {
      val a = sc.parallelize(1 to 9, 3)
      val p = new MyPartitioner()
      val b = a.groupBy((x: Int) => {
         x
      }, p)
   }

   //=============================

   @Test
   def groupBy01(): Unit = {
      val a = sc.parallelize(1 to 9, 3)
      a.groupBy(x => {
         if (x % 2 == 0) "even" else "odd"
      }).collect.foreach(println)
   }


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
