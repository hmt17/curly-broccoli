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
//import com.cloud._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text;


class filec(val filen: String, val offsets:List[Long]) extends Comparable[filec] with Serializable {
    override def compareTo(o:filec):Int = {
        return this.filen.compareTo(o.filen)
    }

    override def toString: String = {
        "{[" + filen + "][" + offsets.mkString(",") + "]}"
    }
};

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
        val outputPath = "/user/djflash/output/scalaout"
        val fs = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(inputPath))
        val output = fs.create(new Path(outputPath))
        val out = new BufferedOutputStream(output)
        var all:RDD[(String, filec)] = sc.emptyRDD[(String, filec)]

        status.foreach(x=> {
            val path:Path = x.getPath
            val filename:String = path.getName

            var lines:RDD[Tuple2[Long, String]] = sc.newAPIHadoopFile(inputPath + filename, classOf[TextInputFormat], classOf[LongWritable],classOf[Text], sc.hadoopConfiguration).map{ case (x:LongWritable, y:Text) => (x.get,y.toString) }
            var pairsPerLine = lines.map{case (x:Long, y:String) => func(x,y)}
            var pairs = pairsPerLine.flatMap(y => y)
            var fullPairs = pairs.map{case(a:Long,b:String) => { println(filename + " " + a.toString + " " + b); (filename,a,b)}}
            fullPairs.collect().foreach(println)

            var separateOffsets = fullPairs.map({case (filename, offset, word) => (word, filename) -> offset})
            var combineOffsets = separateOffsets.groupByKey()
            var properForm = combineOffsets.map{case(x,y) => x._1 -> new filec(x._2, y.toList)}
            all = all.union(properForm)
        })
        println("\r\n\r\nDone with files. combining....\r\n\r\n")

        var combined = all.groupByKey()

        combined.collect().foreach{case(word, iter) => {
                out.write(word.getBytes("UTF-8"))
                out.write("-".getBytes("UTF-8"))
                iter.foreach(x => out.write(x.toString.getBytes("UTF-8")))
                out.write("\r\n".getBytes("UTF-8"))
            }
        }

        out.close()
/*
        println("\r\n Parser output - searching for 'computer'")

        /* works! this is for the parser */
        val parser = new LineParser("hdfs://" + outputPath, "computer")
        parser.parse()
        val result = parser.parserResult.asScala
        result.foreach(a => println(a))
*/
    }
}
