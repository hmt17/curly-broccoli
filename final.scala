import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._
import java.io.File
import java.sql.Timestamp;
import collection.JavaConverters._
import com.cloud._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text;
import scala.collection.mutable._


class filec(val filen: String, val offsets:List[Long]) extends Comparable[filec] with Serializable {
    override def compareTo(o:filec):Int = {
        return this.filen.compareTo(o.filen)
    }

    override def toString: String = {
        "{[" + filen + "][" + offsets.length + "][" + offsets.mkString(",") + "]}"
    }
};

object Main {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Final")
        val sc = new SparkContext(conf)
		
		var exit = true;
		while(exit){
			println("Please input a command:")
			println("1 = Index \t 2 = Search word(s) \t 3 = Search phrase \t 4 = Quit")
			val ln = readLine()
			
			if(ln == "1"){
			//index 
				val indexCompleted = RunIndex(args, sc);
				/*if(!indexCompleted) {
					System.exit(1)
				}*/
			}
			else if(ln == "2"){
			//search word(s)
				Search(0, args)
			}
			else if(ln == "3"){
			//search phrase
				Search(1, args)
			}
			else if(ln == "4"){
			//quit
				exit = false
			}
			else{
			//wrong input
				println("Please enter valid input")
			}
		}
		System.exit(1)
	}
		
def RunIndex(args: Array[String], context: SparkContext) {
        val startTime = System.currentTimeMillis
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

        //val inputPath = "/user/djflash/input/"
        val inputPath = args(0)
        //val outputPath = "/user/djflash/output/scalaout"
        val outputPath:String = args(1) + "scalaout"
        val fs = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(inputPath))
        val output = fs.create(new Path(outputPath))
        val out = new BufferedOutputStream(output)
        var all:RDD[(String, filec)] = context.emptyRDD[(String, filec)]

        status.foreach(x=> {
            val path:Path = x.getPath
            val filename:String = path.getName

            var lines:RDD[Tuple2[Long, String]] = context.newAPIHadoopFile(inputPath + filename, classOf[TextInputFormat], classOf[LongWritable],classOf[Text], context.hadoopConfiguration).map{ case (x:LongWritable, y:Text) => (x.get,y.toString) }
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
		val endTime = System.currentTimeMillis
		println("Difference: " + (endTime - startTime))
	}
     def Search(wordOrPhrase: Int, args: Array[String]) {
        val startTime = System.currentTimeMillis
        var input:String = ""
        var saveOutput: String = ""
        if(wordOrPhrase == 0) {
            input = readLine("Please enter the words to search (seperated by a single space):")
        }
        else {
            input = readLine("Please enter the phrase to search:")
        }

        while(saveOutput == "") {
            saveOutput = readLine("Would you like to save your results? Your output will be saved to your output directory with file name 'last_results'. Please enter 'y' or 'n'.")
                    
            if(saveOutput != "y" && saveOutput != "n") {
                saveOutput = ""
            }
        }


        println("\r\n Parser output - searching for '" + input + "'")

        var totalList:ArrayBuffer[ParserReturnable] = new ArrayBuffer[ParserReturnable]()
        var searchingForOffsets:ArrayBuffer[ParserReturnable] = new ArrayBuffer[ParserReturnable]()

        val terms:Array[String] = input.split(" ")

        /* HARDCODED OUTPUT PATH */
        terms.foreach(term => {
            println("searching term: " + term)
            var foundInTotalList:Boolean = false
            val outputPath:String = args(1) + "scalaout"
            val parser = new LineParser("hdfs://" + outputPath, term)
            parser.parse()
            val result = parser.parserResult.asScala

            /* check if we're doing word searches or phrasing */
            if(wordOrPhrase == 0) {
                result.foreach(returnable => {
                    totalList.foreach(returned => {
                        if(returned.filename == returnable.filename) {
                            foundInTotalList = true
                            returned.numOfOccur = (returned.numOfOccur + returnable.numOfOccur)
                        }
                    })

                    if(foundInTotalList == false) {
                        println("adding filename: " + returnable.filename)
                        totalList += returnable
                    }
                    foundInTotalList = false
                })
            }
            else {
                if(searchingForOffsets.size == 0) {
                    result.foreach(a => {
                        var pr: ParserReturnable = new ParserReturnable()
                        pr.filename = a.filename
                        pr.numOfOccur = a.numOfOccur
                        val offsets = a.offsets.asScala
                        offsets.foreach{case (offset:Integer) => {
                                pr.offsets.add(offset + term.length() + 1)
                                
                        }}
                        searchingForOffsets += pr
                    })
                }
                else {

                    var temp: ArrayBuffer[ParserReturnable] = new ArrayBuffer[ParserReturnable]()

                    result.foreach(output => {
                        searchingForOffsets.foreach(last => {
                            if(output.filename == last.filename) {
                                output.offsets.retainAll(last.offsets)
                                if(output.offsets.size > 0) {
                                    temp += new ParserReturnable(last.filename, output.offsets.size, output.offsets)
                                }
                            }
                        })
                    }) /* result foreach */
                    searchingForOffsets = new ArrayBuffer[ParserReturnable]()
                    searchingForOffsets = temp.clone() /* copy */
                } /* searchingoffsets else */
            } /* wordOrPhrase else */
        }) /* terms foreach */

        val endTime = System.currentTimeMillis


        if(wordOrPhrase == 0) {
            if(saveOutput == "y") {
                saveOutputFile(totalList, args)
            }
            printOutput(totalList)
        }
        else {
            if(saveOutput == "y") {
                saveOutputFile(searchingForOffsets, args)
            }
            printOutput(searchingForOffsets)
        }
        
		println("Difference: " + (endTime - startTime))
    }

    def printOutput(list: ArrayBuffer[ParserReturnable]) {
        val cmp = (x:ParserReturnable, y:ParserReturnable) => x.numOfOccur > y.numOfOccur
        val sorted = scala.util.Sorting.stableSort(list, cmp)
        sorted.foreach(println)
    }

def saveOutputFile(list: ArrayBuffer[ParserReturnable], args:Array[String]) {
        val outputPath:String = args(1) + "last_results"
        val fs = FileSystem.get(new Configuration())
        val output = fs.create(new Path(outputPath))
        val out = new BufferedOutputStream(output)
        out.write("------ Output File -----\r\n".getBytes("UTF-8"))

        list.foreach(returnable => {
            out.write(returnable.toString.getBytes("UTF-8"))
            out.write("\r\n".getBytes("UTF-8"))
        })

        out.write("----- Finished -----".getBytes("UTF-8"))
        out.write("\r\n".getBytes("UTF-8"))
        out.close()
    }
}

