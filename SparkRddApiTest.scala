package com.scathon.spark.rdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.junit.Test

/**
  * @Description Spark RDD Learning
  * @Auhtor linhuadong(ScathonLin)
  * @Note Spark Test Environmemt->2.2.2
  *       Local Mod + Spark Shell
  *       阅读代码的时候，从后往前看，具体的RDD是按照字母顺序来的
  */
class RddDemo {
  val conf = new SparkConf().setAppName("rdd").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")


  /**
    * 返回运行的DEBUG信息
    */
  @Test
  def toDebugString(): Unit = {
    val a = sc.parallelize(1 to 9, 3)
    val b = sc.parallelize(1 to 3, 3)
    val c = a.subtract(b)
    c.toDebugString
    /*
    res9: String =
      (3) MapPartitionsRDD[26] at subtract at <console>:28 []
       |  SubtractedRDD[25] at subtract at <console>:28 []
       +-(3) MapPartitionsRDD[23] at subtract at <console>:28 []
       |  |  ParallelCollectionRDD[21] at parallelize at <console>:24 []
       +-(3) MapPartitionsRDD[24] at subtract at <console>:28 []
          |  ParallelCollectionRDD[22] at parallelize at <console>:24 []
     */
  }

  /**
    * 返回确定数量的抽样数据。注意，返回的是Array而不是RDD，然后内存会随机打乱返回的元素的顺序
    */
  @Test
  def takeSample(): Unit = {
    val x = sc.parallelize(1 to 1000, 3)
    x.takeSample(true, 100, 1)
    //spark shell 运行结果
    /*
    res8: Array[Int] = Array(764, 815, 274, 452, 39, 538, 238, 544, 475, 480, 416, 868, 517, 363, 39, 316, 37,
     90, 210, 202, 335, 773, 572, 243, 354, 305, 584, 820, 528, 749, 188, 366, 913, 667, 214, 540, 807, 738,
     204, 968, 39, 863, 541, 703, 397, 489, 172, 29, 211, 542, 600, 977, 941, 923, 900, 485, 575, 650, 258, 31,
      737, 155, 685, 562, 223, 675, 330, 864, 291, 536, 392, 108, 188, 408, 475, 565, 873, 504, 34, 343, 79, 493,
       868, 974, 973, 110, 587, 457, 739, 745, 977, 800, 783, 59, 276, 987, 160, 351, 515, 901)
     */
  }

  /**
    * 对RDD里面的元素进行排序然后取出前n个元素
    */
  @Test
  def takeOrdered(): Unit = {
    val b = sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
    b.takeOrdered(2).foreach(println)
  }

  /**
    * 抽取RDD中前n个元素
    */
  @Test
  def take(): Unit = {
    val b = sc.parallelize(List("dog", "cat", "ape", "salmon", "gnu"), 2)
    b.take(2).foreach(printf("%-3s", _))
    println()
    val c = sc.parallelize(1 to 10000, 5000)
    c.take(100).foreach(printf("%-3d", _))
    println()
  }

  @Test
  def sum(): Unit = {
    //....
  }

  /**
    * 根据key求解差集
    */
  @Test
  def subtractByKey(): Unit = {
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider"), 2)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
    val d = c.keyBy(_.length)
    b.subtractByKey(d).collect.foreach(println)
  }

  /**
    * 求差集 A-B
    */
  @Test
  def subtract(): Unit = {
    val a = sc.parallelize(1 to 9, 3)
    val b = sc.parallelize(1 to 3, 3)
    val c = a.subtract(b)
    c.collect.foreach(println)
  }

  /**
    * 求解标准差
    */
  @Test
  def stdev(): Unit = {
    val d = sc.parallelize(List(0.0, 1.0), 3)
    println(d.stats())
    println(d.stdev())
  }

  /**
    * 根据key进行排序
    */
  @Test
  def sortByKey(): Unit = {
    val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val b = sc.parallelize(1 to a.count.toInt, 2)
    val c = a.zip(b)
    c.sortByKey(false).collect.foreach(println)
    println()
    c.sortByKey(true).collect.foreach(println)
    println("one more complex example...")
    val d = sc.parallelize(1 to 100, 5)
    val e = d.cartesian(a)
    //进行数据抽样
    val f = sc.parallelize(e.takeSample(true, 5, 13), 2)
    val g = f.sortByKey(false)
    g.collect.foreach(println)
  }

  /**
    * 排序函数
    * 可以指定排序规则
    */
  @Test
  def sortBy(): Unit = {
    val y = sc.parallelize(List(5, 7, 1, 3, 2, 1))
    // 根据数值大小排序
    y.sortBy(item => item, true).collect.foreach(printf("%-2d", _))
    println()
    val z = sc.parallelize(List("linhd", "scathon", "scala", "javascript", "python", "go", "c"))
    val sortFunction = (elem: String) => elem.length
    // 根据字符串的长度作为排序依据,降序排序
    z.sortBy(sortFunction, ascending = false).collect.foreach(printf("%-12s", _))
  }

  /**
    * 同时计算平均数，元素个数，标准差指标的数据
    */
  @Test
  def stats(): Unit = {
    val x = sc.parallelize(List(1.0, 2.0, 3.0, 5.0), 2)
    println(x.stats())
  }

  /**
    * 以hadoop file的文件形式保存
    */
  @Test
  def saveAsHadoopFile(): Unit = {
    //...
  }


  /**
    * 以文本文件的形式保存文件
    */
  @Test
  def saveAsTextFile(): Unit = {
    //....
  }

  /**
    * 以序列文件的形式保存文件
    */
  @Test
  def saveAsSequenceFIle(): Unit = {
    val v = sc.parallelize(Array(("pwl", 3), ("gnu", 4), ("dog", 1), ("cat", 2), ("ant", 5)), 2)
    v.saveAsSequenceFile("hd_seq_file")
  }

  /**
    * 以对象文件的形式保存文件
    */
  @Test
  def saveAsObjectFile(): Unit = {
    val x = sc.parallelize(1 to 100, 3)
    x.saveAsObjectFile("objectFile")
    val y = sc.objectFile[Int]("objectFile")
    y.collect.foreach(println)
  }

  /**
    * 根据key进行抽样，通过指定期望的key出现的比率进行抽样。
    */
  @Test
  def sampleByKey(): Unit = {
    val randRDD = sc.parallelize(List((7, "cat"), (6, "mouse"), (7, "cup"), (6, "book"), (7, "tv"), (6, "screen"), (7, "heater")))
    // key是7出现的概率是0.4，key是6出现的概率是0.6
    val sampleMap = List((7, 0.4), (6, 0.6)).toMap
    randRDD.sampleByKey(false, sampleMap, 42).collect.foreach(println)
  }

  /**
    * 抽样数据
    */
  @Test
  def sample(): Unit = {
    val a = sc.parallelize(1 to 10000, 3)

    /**
      * 参数解释：
      * withReplacement: 一个元素是否可以被取样多次（多次被取样的时候会更换）
      * fraction，占全部元素个数的比率
      * seed，产生随机数的种子
      */
    println(a.sample(false, 0.1, 0).count())
    println(a.sample(true, 0.3, 0).count())
    println(a.sample(true, 0.3, 13).count())
  }

  /**
    * 右外连接
    */
  @Test
  def rightOuterJoin(): Unit = {
    val a = sc.parallelize(List("dog", "slamon", "rat", "elephant"))
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("dog", "gnu", "slamon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val d = c.keyBy(_.length)
    b.rightOuterJoin(d).collect.foreach(println)
  }

  /**
    * 根据指定的分区函数，将RDD进行重新分区，并且将重新分区之后的RDD的每个分区之内的元素根据Key进行排序操作
    */
  @Test
  def repartitionAndSortWithinPartitions(): Unit = {
    val randRDD = sc.parallelize(List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater")), 3)
    val rPartitioner = new RangePartitioner(3, randRDD)
    val partitioned = randRDD.partitionBy(rPartitioner)

    def myfunc(index: Int, iter: Iterator[(Int, String)]): Iterator[String] = {
      iter.map(x => "[partID: " + index + ", val: 0" + x + "]")
    }
    // 本次测试运行不起来，在spark shell中运行结果如下：
    //res2: Array[String] = Array([partID: 0, val: 0(2,cat)], [partID: 0, val: 0(3,book)],
    // [partID: 0, val: 0(1,screen)], [partID: 1, val: 0(4,tv)], [partID: 1, val: 0(5,heater)],
    // [partID: 2, val: 0(6,mouse)], [partID: 2, val: 0(7,cup)])
    //
    partitioned.mapPartitionsWithIndex(myfunc).collect
    val partitioned1 = randRDD.repartitionAndSortWithinPartitions(rPartitioner)
    partitioned1.mapPartitionsWithIndex(myfunc).collect
    /*
    * 分区内的元素是有序的。
    * res5: Array[String] = Array([partId: 0, val: (1,screen)], [partId: 0, val: (2,cat)], [partId: 0, val: (3,book)],
    * [partId: 1, val: (4,tv)], [partId: 1, val: (5,heater)],
    * [partId: 2, val: (6,mouse)], [partId: 2, val: (7,cup)])
    * */
  }

  /**
    * 重新分区
    */
  @Test
  def repartition(): Unit = {
    val rdd = sc.parallelize(List(1, 2, 10, 4, 5, 2, 1, 1, 1), 3)
    println(rdd.partitions.length)
    val rdd2 = rdd.repartition(4)
    println(rdd2.partitions.length)
  }

  /**
    * 通过key进行reduce操作。一般操作的是(k,v)tuple
    */
  @Test
  def reduceByKey(): Unit = {
    val rdd1 = sc.parallelize(List("linhd", "scathon", "kobe", "james"), 3)
    val rdd2 = rdd1.keyBy(_.length)
    rdd2.reduceByKey(_ + ":" + _).collect.foreach(println)
  }

  @Test
  def reduce(): Unit = {
    val rdd1 = sc.parallelize(List("linhd", "scathon", "kobe", "james"), 3)
    val rdd2 = rdd1.reduce(_ + ":" + _)
    println(rdd2)
  }

  /**
    * 根据权重数组将RDD随机分割为多个较小的RDDs，
    * 权重数组指定分配给每个较小RDD的数据元素总数的百分比。
    * 注意，每个较小的RDD的实际大小仅近似等于百分比
    */
  @Test
  def randomSplit(): Unit = {
    val y = sc.parallelize(1 to 10)
    val splits = y.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    training.collect.foreach(println)
    println("=====================")
    test.collect.foreach(println)
    println("====================")
    val z = sc.parallelize(1 to 10)
    val splits1 = z.randomSplit(Array(0.1, 0.3, 0.6))
    val rdd1 = splits1(0)
    val rdd2 = splits1(1)
    val rdd3 = splits1(2)
    rdd1.collect.foreach(println)
    println("--------------")
    rdd2.collect.foreach(println)
    println("**************")
    rdd3.collect.foreach(println)
    println("^^^^^^^^^^^^^^")

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
