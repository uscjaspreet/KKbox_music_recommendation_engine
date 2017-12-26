package task2
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io._

object CollaborativeFiltering {
  val conf = new SparkConf().setAppName("TASK2").setMaster("local[10]")
  val sc = new SparkContext(conf)
  var similarityUsers: Map[(Int, Int), Double] = Map()
  var meanData: Map[Int, Double] = Map()
  var movieData: Map[Int, Iterable[(Int, Double)]] = Map()
  def main(args: Array[String]) {
    val fileInput = args(0)
    val testFile = args(1)
    movieData = sc.textFile(fileInput)
      .map(line => ((line.split(",")(1), line.split(",")(0)), line.split(",")(2)))
      // key value pair for user- distinct movie
      .fullOuterJoin(sc.textFile(testFile).
        map(line => ((line.split(",")(1), line.split(",")(0)), 1)))
      .filter(x => x._2._1.isEmpty || x._2._2.isEmpty)
      .map {
        case (k, v) => (k._1.toInt, (k._2.toInt, v._1.get.toDouble))
      }
      .groupByKey()
      .collect()
      .toMap

    val start_time = System.nanoTime()
    //generating baskets from files
    val trainData = sc.textFile(fileInput)
      .map(line => ((line.split(",")(0), line.split(",")(1)), line.split(",")(2)))
      // key value pair for user- distinct movie
      .fullOuterJoin(sc.textFile(testFile).
        map(line => ((line.split(",")(0), line.split(",")(1)), 1)))
      .filter(x => x._2._1.isEmpty || x._2._2.isEmpty)
      .map {
        case (k, v) => (k._1.toInt, (k._2, v._1.get.toDouble))
      }
      .groupByKey()
      .collect()
      .toMap

    similarityUsers = similar_user(trainData)
    for (t <- trainData) {
      val values = t._2.toArray
      val average = (values.map {
        x => x._2
      }
        .sum) / (values.map {
          x => x._2
        }
          .length)

      meanData = meanData + (t._1 -> average)
    }
    val testData = sc.textFile(testFile).
      map(line => ((line.split(",")(0), line.split(",")(1))))
    val header = testData.first()
    val testing = testData.filter(row => row != header)
      .map {
        case (k, v) => ((k.toInt, v.toInt), 0.0)
      }
    val PredictionOutput = testing.mapPartitions(x => predictions(x), true)
    val predictedData = PredictionOutput.sortByKey().collect()
    val totalData = sc.parallelize(sc.textFile(fileInput)
      .map(line => ((line.split(",")(0), line.split(",")(1)), line.split(",")(2)))
      // key value pair for user- distinct movie
      .collect().toList)
    val headerTotal = totalData.first()
    val testDataResults = totalData.filter(row => row != headerTotal)
      .join(sc.textFile(testFile).
        map(line => ((line.split(",")(0), line.split(",")(1)), 1)))
      .map {
        case (k, v) => ((k._1.toInt, k._2.toInt), v._1.toDouble)
      }
    val header1 = testDataResults.first()
    val ratesAndPreds = testDataResults.filter(row => row != header)
      .join(PredictionOutput)
    ratesAndPreds.cache()
    //ratesAndPreds.saveAsTextFile("testFile")
    val absoluteErrorData = ratesAndPreds
      .map {
        case (k, v) => (k, Math.abs(v._1 - v._2))
      }
    absoluteErrorData.cache()
    for (i <- absoluteErrorData) {

    }
    val MSE = absoluteErrorData.map {
      case ((user, product), error) =>
        error * error
    }.mean()
    val counts = Array[Int](0, 0, 0, 0, 0)
    ratesAndPreds.collect.foreach {
      case ((user, product), (r1, r2)) => {
        val absDiff = Math.abs(r1 - r2)
        if (absDiff >= 0.0 && absDiff < 1.0) {
          counts(0) += 1
        } else {
          if (absDiff >= 1.0 && absDiff < 2.0) {
            counts(1) += 1
          } else {
            if (absDiff >= 2.0 && absDiff < 3.0) {
              counts(2) += 1
            } else {
              if (absDiff >= 3.0 && absDiff < 4.0) {
                counts(3) += 1
              } else {
                counts(4) += 1
              }
            }
          }
        }
      }
    }

    val end_time = System.nanoTime()
    sc.stop
    using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("Jaspreet_Singh_Result_Task2.txt")))) {
      writer =>
        for (l <- predictedData) {
          writer.write(l.toString() + '\n') // however you want to format it
        }
    }
    println(">=0 and <1: " + counts(0))
    println(">=1 and <2: " + counts(1))
    println(">=2 and <3: " + counts(2))
    println(">=3 and <4: " + counts(3))
    println(">=4: " + counts(4))
    println("RMSE = " + Math.sqrt(MSE))
    println("The total execution time taken is " + (end_time - start_time) * Math.pow(10, -9) + " sec.")
  }
  def using[T <: Closeable, R](resource: T)(block: T => R): R = {
    try { block(resource) }
    finally { resource.close() }
  }
  def predictions(b: Iterator[((Int, Int), Double)]): Iterator[((Int, Int), Double)] = {
    var pred: Map[(Int, Int), Double] = Map()
    val temp = Iterable((1, 0.0))
    for (userMovie <- b) {
      val mean = meanData.getOrElse(userMovie._1._1, -999.0)
      var numerator = 0.0
      var denominator = 0.0
      if (mean != 999.0) {
        val neighbors = movieData.getOrElse(userMovie._1._2, temp)
        for (n <- neighbors) {
          if (n != (1,0.0)) {
            val simUser = similarityUsers.getOrElse((userMovie._1._1, n._1), 0.0)

            denominator = denominator + Math.abs(simUser)
            numerator = numerator + ((n._2 - meanData.getOrElse(n._1, -999.0)) * simUser)
          }
        }
        if (denominator != 0.0) {
          var predictionOutput = mean + (numerator / denominator)
          if (predictionOutput < 0)
            predictionOutput = 0
          if (predictionOutput > 5)
            predictionOutput = 5
          pred = pred + (userMovie._1 -> predictionOutput)
        } else pred = pred + (userMovie._1 -> mean)
      }
    }
    pred.toIterator
  }
  def similar_user(tData: Map[Int, Iterable[(String, Double)]]): Map[(Int, Int), Double] = {
    val tDatatemp = tData
    var userWeights: Map[(Int, Int), Double] = Map()
    for (a <- tData) {
      for (u <- tDatatemp) {
        if ((a._2.size > 0 && u._2.size > 0) && (a._1 != u._1)) {
          val pearCorrOption = pearsonCorrelationScore(a._2.toMap, u._2.toMap)
          val pearCorr = pearCorrOption match {
            case Some(a) => a
            case None    => 999.0
          }
          if (pearCorr != 999.0)
            userWeights = userWeights + ((a._1, u._1) -> pearCorr)
        }
      }
    }
    userWeights
  }

  def commonMapKeys[A, B](a: Map[A, B], b: Map[A, B]): Set[A] = a.keySet.intersect(b.keySet)
  def pearsonCorrelationScore(
    p1MovieRatings: Map[String, Double],
    p2MovieRatings: Map[String, Double]): Option[Double] = {

    // find the movies common to both reviewers
    val listOfCommonMovies = commonMapKeys(p1MovieRatings, p2MovieRatings).toSeq
    val n = listOfCommonMovies.size
    if (n == 0) return None

    // reduce the maps to only the movies both reviewers have seen
    val p1CommonMovieRatings = p1MovieRatings.filterKeys(movie => listOfCommonMovies.contains(movie))
    val p2CommonMovieRatings = p2MovieRatings.filterKeys(movie => listOfCommonMovies.contains(movie))

    // add up all the preferences
    val sum1 = p1CommonMovieRatings.values.sum
    val sum2 = p2CommonMovieRatings.values.sum

    // sum up the squares
    val sum1Sq = p1CommonMovieRatings.values.foldLeft(0.0)(_ + Math.pow(_, 2))
    val sum2Sq = p2CommonMovieRatings.values.foldLeft(0.0)(_ + Math.pow(_, 2))

    // sum up the products
    val pSum = listOfCommonMovies.foldLeft(0.0)((accum, element) => accum + p1CommonMovieRatings(element) * p2CommonMovieRatings(element))

    // calculate the pearson score
    val numerator = pSum - (sum1 * sum2 / n)
    val denominator = Math.sqrt((sum1Sq - Math.pow(sum1, 2) / n) * (sum2Sq - Math.pow(sum2, 2) / n))
    if (denominator == 0) None else Some(numerator / denominator)
  }
}