import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._
import java.io.File
import collection.JavaConverters._
//import com.cloud._ this is for the parser
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text;

object Main {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Final")
        //val sc = new SparkContext(conf)


        /* this needs to be a method not a function b/c by itself it is not serializable and will error out! */
        val func = (line:(Long,String)) => {
            val words = line._2.split(" ");
            var offset:Long = line._1;
            val test = words.map(word => {
                val returnVal = offset
                offset = offset + word.length + 1
                (returnVal, word)
            }
            )
            test
        }
        println("Hello world")

        val inputPath = "/user/djflash/input/"
        val fs = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(inputPath))

        status.foreach(x=> {
            val path:Path = x.getPath
            val filename:String = path.getName

            val lines:RDD[Tuple2[Long, String]] = sc.newAPIHadoopFile(inputPath + filename, classOf[TextInputFormat], classOf[LongWritable],classOf[Text], sc.hadoopConfiguration).map{ case (x:LongWritable, y:Text) => (x.get,y.toString) }
            val pairsPerLine = lines.map{case (x:Long, y:String) => func(x,y)}
            val pairs = pairsPerLine.flatMap(y => y)
val fullPairs = pairs.map{case(a:Long,b:String) => { println(filename + " " + a.toString + " " + b); (filename,a,b)}}
            fullPairs.collect().foreach(println)

        })



        /* works! this is for the parser
        val parser = new LineParser("hdfs:///user/djflash/output/part-r-00000", "computer")
        parser.parse()
        val result = parser.parserResult.asScala
        println("Hello world")
        result.foreach(a => println(a))
        */
    }
}
