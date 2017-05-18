package hdfs
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Runner {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Yaruliy Count"))
    val threshold = args(1).toInt
    val file = sc.textFile(args(0))

    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    System.out.println("File Exists: " + fs.exists(new Path(args(0))))
    val iterator = fs.listFiles(new Path("hdfs://quickstart.cloudera:8020/"), false)
    System.out.println("Files: ")
    while (iterator.hasNext){ System.out.println("\t + " + iterator.next()) }
    System.out.println("fs.getHomeDirectory: " + fs.getHomeDirectory)

    if(fs.exists(new Path(args(0)))){
      val tokenized = file.flatMap(_.split(" "))
      val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
      val filtered = wordCounts.filter(_._2 >= threshold)
      val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
      System.out.println(charCounts.collect().mkString(", "))
    }
    println("End of Execution")
  }
}