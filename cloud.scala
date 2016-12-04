import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._

println("Hello World!!!!")
//val conf = new SparkConf().setAppName("cloudFinal")
//val sc = new SparkContext(conf)
val fileName = "test.txt"
val fileName1 = "test1.txt"
val input =  sc.textFile(fileName)
val input1 =  sc.textFile(fileName1)

val out = "output.txt"
val ofile = new File(out)
val output = new BufferedWriter(new FileWriter(ofile))



val words = input.flatMap(line => line.split(" "))
val counts = words.map(word => ( word, 1)).reduceByKey{case (x, y) => x + y}
val cout = counts.map(word => (fileName, word._1, word._2) )

val words1 = input1.flatMap(line => line.split(" "))
val counts1 = words1.map(word => ( word, 1)).reduceByKey{case (x, y) => x + y}
val cout1 = counts1.map(word => (fileName1, word._1, word._2) )
val c = cout.union(cout1).collect
//counts.saveAsTextFile(output)
c.foreach{ i =>
  output.write("( " + i._1 + " , " + i._2 + " , " + i._3+ " )\n")
  //println("( " + i._1 + " , " + i._2 + " )")
}
output.close()
val bob = "a" //prompt user by in java
val boobies = cout.filter(_._2.equals(bob))
