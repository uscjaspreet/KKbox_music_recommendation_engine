package Assignment2
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
import scala.io.Source
import java.io._
object SONApriori {
  val conf = new SparkConf().setAppName("SON-Apriori").setMaster("local[4]")
  val sc = new SparkContext(conf)
  var phase1Result: Array[List[Int]] = Array()
  def main(args: Array[String]) {
    val t1 = System.nanoTime
    //generating baskets from files
    val support = args(3).toInt
    val ratingsFile = args(1)
    val usersFile = args(2)
    if (args(0) == "1") {
      case1(support, ratingsFile, usersFile)
    } else
      case2(support, ratingsFile, usersFile)
    println((System.nanoTime - t1) / 1e9d)
  }
  def case1(support: Int, ratingsFile: String, usersFile: String) {
    val baskets = sc.parallelize(sc.textFile(ratingsFile)
      .map(line => (line.split("::")(0),
        (line.split("::")(1)))).distinct()
      // key value pair for user- distinct movie
      .join(sc.textFile(usersFile).
        map(line => (line.split("::")(0),
          line.split("::")(1)))) // key value pair for user - gender
      .filter(x => x._2._2 == "M")
      .map {
        case (k, v) => (k.toInt, v._1.toInt) //key value pair for user-movie
      }.groupByKey() // collecting all the movies by each user
      .values.collect().toList)
    baskets.cache()
    val partitions = baskets.getNumPartitions
    println(partitions)
    phase1Result = baskets
      .mapPartitions(iter => phase1(iter, partitions, support), true)
      .distinct()
      .collect()
    val phase2Result = baskets
      .mapPartitions(iter => phase2(iter), true)
      .reduceByKey((x1, x2) => x1 + x2)
      .filter(x => x._2 > support)
      .map(x => (x._1.size, x._1))
      .groupByKey
      .sortByKey()
      .collect()
    sc.stop
    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("OutputFiles/Siddharth_Jain_SON.case1_" + support + ".txt")))) {
      writer =>
        for (l <- phase2Result) {
          writer.write((sort(l._2.toSeq)).toString().replaceAll("List", "") + '\n') 
        }
    }
  }
  def case2(support: Int, ratingsFile: String, usersFile: String) {
    val baskets = sc.parallelize(sc.textFile(ratingsFile)
      .map(line => (line.split("::")(0),
        (line.split("::")(1)))).distinct()
      // key value pair for user- distinct movie
      .join(sc.textFile(usersFile).
        map(line => (line.split("::")(0),
          line.split("::")(1)))) // key value pair for user - gender
      .filter(x => x._2._2 == "F")
      .map {
        case (k, v) => (v._1.toInt, k.toInt) //key value pair for user-movie
      }.groupByKey() // collecting all the movies by each user
      .values.collect().toList)
    baskets.cache()
    val partitions = baskets.getNumPartitions
    println(partitions)
    phase1Result = baskets
      .mapPartitions(iter => phase1(iter, partitions, support), true)
      .distinct()
      .collect()
    val phase2Result = baskets
      .mapPartitions(iter => phase2(iter), true)
      .reduceByKey((x1, x2) => x1 + x2)
      .filter(x => x._2 > support)
      .map(x => (x._1.size, x._1))
      .groupByKey
      .sortByKey()
      .collect()
    sc.stop
    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("OutputFiles/Siddharth_Jain_SON.case2_" + support + ".txt")))) {
      writer =>
        for (l <- phase2Result) {
          writer.write((sort(l._2.toSeq)).toString().replaceAll("List", "") + '\n') 
        }
    }
  }
  def using[T <: Closeable, R](resource: T)(block: T => R): R = {
    try { block(resource) }
    finally { resource.close() }
  }
  def phase1(b: Iterator[Iterable[Int]], partitions: Int, s: Int): Iterator[List[Int]] = {
    var itemList: List[List[Int]] = List()
    val localSupport = s / partitions
    for (l <- b) {
      itemList = itemList :+ l.toList
    }
    var currentItemList: List[Int] = List()
    var currentLList: List[List[Int]] = List()
    var candidates: List[List[Int]] = List()
    var k: Int = 1
    var n = 1
    var itemCombs: Iterator[List[Int]] = Iterator()
    while (n != 0) {
      val countMap: Map[List[Int], Int] = Map()
      itemCombs = Iterator()
      for (i <- itemList) {
        itemCombs = itemCombs ++ i.combinations(k)
      }
      currentItemList = List()
      for (i <- itemCombs) {
        val j = i.sorted
        if (countMap.contains(j))
          countMap(j) = countMap.getOrElse(j, 0) + 1
        else
          countMap += j -> 1
      }
      for (cm <- countMap) {
        if (cm._2 >= localSupport) {
          candidates = candidates :+ cm._1
          currentItemList = currentItemList ::: cm._1
        }
      }
      currentLList = List()
      if (currentItemList.size > 0) {
        for (i <- itemList) {
          val j = i.intersect(currentItemList)
          currentLList = currentLList :+ j
        }
        itemList = currentLList
        n = 1
        k += 1
      } else
        n = 0
    }
    candidates.toIterator
  }
  def phase2(b: Iterator[Iterable[Int]]): Iterator[(List[Int], Int)] = {
    var itemList: List[List[Int]] = List()
    for (l <- b) {
      itemList = itemList :+ l.toList
    }
    var candidates: List[(List[Int], Int)] = List()
    val countMap: Map[List[Int], Int] = Map()
    for (i <- phase1Result) {
      for (j <- itemList) {
        if (i.toSet.subsetOf(j.toSet)) {
          if (countMap.contains(i))
            countMap(i) = countMap.getOrElse(i, 0) + 1
          else
            countMap += i -> 1
        }
      }
    }
    for (cm <- countMap) {
      candidates = candidates :+ cm
    }
    candidates.toIterator
  }
  def sort[A: Ordering](coll: Seq[Iterable[A]]) = coll.sorted
}