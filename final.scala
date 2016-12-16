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
import com.cloud._ /* Custom classes */
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text;
import scala.collection.mutable._

/* Custom class to represent a filename & it's associated offsets key-value pairs. It is Serializable
   to ensure use with RDD */
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
        /* Create configuration and Context */
        val conf = new SparkConf().setAppName("Final")
        val sc = new SparkContext(conf)
		
		var exit = true;

        /* continue asking for input until the user signals for a quit */
		while(exit){
			println("Please input a command:")
			println("1 = Index \t 2 = Search word(s) \t 3 = Search phrase \t 4 = Quit")
			val ln = readLine()
			
			if(ln == "1"){
				val indexCompleted = RunIndex(args, sc);
			}
			else if(ln == "2"){
			    /* search word(s) */
				Search(0, args)
			}
			else if(ln == "3"){
                /* search phrase */
				Search(1, args)
			}
			else if(ln == "4"){
                /* quit */
				exit = false
			}
			else{
                /* wrong input */
				println("Please enter valid input")
			}
		}

		System.exit(1)
	}
	
    /* Function to create the inverted index and its output file. The output file will be located in the output directory specified with the filename 'scalaout' */
    def RunIndex(args: Array[String], context: SparkContext) {

        val inputPath = args(0)
        val outputPath:String = args(1) + "scalaout"
        val fs = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(inputPath))
        val output = fs.create(new Path(outputPath))
        val out = new BufferedOutputStream(output)
        var all:RDD[(String, filec)] = context.emptyRDD[(String, filec)]

        /* start timer */
        val startTime = System.currentTimeMillis
        

        /*  Method receives a line of the input file and outputs an offset for each individual word in the line.
            NOTE: this needs to be a method not a function b/c by itself it is not serializable and will error out! */
        val func = (line:(Long,String)) => {
            /* split all words */
            val words = line._2.split(" ");

            /* variable to keep track of offsets as we travserse the words */
            var offset:Long = line._1;

            /* traverse words in the line, emitting the word with its calculated index */
            val test = words.map(word => {
                    val returnVal = offset
                    val lowerCaseWord = word.toLowerCase
                    /* new offset accounts for the length of the word plus a space character */
                    offset = offset + word.length + 1
                    (returnVal, lowerCaseWord)
                }
            )
            test
        }

        /* traverse the files in the input path */
        status.foreach(x=> {
            val path:Path = x.getPath
            val filename:String = path.getName

            /* get lines and their offsets within the file */
            var lines:RDD[Tuple2[Long, String]] = context.newAPIHadoopFile(inputPath + filename, classOf[TextInputFormat], classOf[LongWritable],classOf[Text], context.hadoopConfiguration).map{ case (x:LongWritable, y:Text) => (x.get,y.toString) }

            /* separate words from lines for <offset, word> pairs */
            var pairsPerLine = lines.map{case (x:Long, y:String) => func(x,y)}

            /* flatten */
            var pairs = pairsPerLine.flatMap(y => y)

            /* add in filename for each <offset, word> pair to create 3-tuples */
            var fullPairs = pairs.map{case(a:Long,b:String) => { println(filename + " " + a.toString + " " + b); (filename,a,b)}}
            //fullPairs.collect().foreach(println)

            /* create key-value pairs where the key is the word and filename in order group and have a list of offsets for each word, filename pair */
            var separateOffsets = fullPairs.map({case (filename, offset, word) => (word, filename) -> offset})
            var combineOffsets = separateOffsets.groupByKey()

            /* creates key-value pair where key is the word with a list of <filename, offsets> pairs FOR THIS FILENAME */
            var properForm = combineOffsets.map{case(x,y) => x._1 -> new filec(x._2, y.toList)}

            /* combine with other pairs */
            all = all.union(properForm)
        })

        println("\r\n\r\nDone with files. combining....\r\n\r\n")

        /* combine all words FROM ALL FILES to have a the inverted index: word - <document1, offset: 1,2> */
        var combined = all.groupByKey()

        /* write output file */
        combined.collect().foreach{case(word, iter) => {
                out.write(word.getBytes("UTF-8"))
                out.write("-".getBytes("UTF-8"))
                iter.foreach(x => out.write(x.toString.getBytes("UTF-8")))
                out.write("\r\n".getBytes("UTF-8"))
            }
        }

        /* stop time */
        val endTime = System.currentTimeMillis

        out.close()
		println("Difference: " + (endTime - startTime))
	}

    /* perform search on the inverted index */
    def Search(wordOrPhrase: Int, args: Array[String]) {
        var input:String = ""
        var saveOutput: String = ""
        
        /* ask for input */
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


        println("\r\nParser output - searching for '" + input + "'")

        /* start timer */
        val startTime = System.currentTimeMillis

        /* variable to track all results (used for regular search) */
        var totalList:ArrayBuffer[ParserReturnable] = new ArrayBuffer[ParserReturnable]()

        /* variable to track results for phrasing */
        var searchingForOffsets:ArrayBuffer[ParserReturnable] = new ArrayBuffer[ParserReturnable]()

        /* split terms of user input */
        val terms:Array[String] = input.split(" ")

        terms.foreach(term => {
            var foundInTotalList:Boolean = false
            val outputPath:String = args(1) + "scalaout"

            /* create new parser object (found in com.cloud.LineParser) */
            val parser = new LineParser("hdfs://" + outputPath, term)
            parser.parse()

            /* convert ArrayList from parser.parserResult to ArrayBuffer in scala - there's a direct conversion */
            val result = parser.parserResult.asScala

            /* check if we're doing word searches or phrasing */
            if(wordOrPhrase == 0) {
                result.foreach(returnable => {
                    totalList.foreach(returned => {
                        /* If we have seen this file before, add the occurences together to show the number of times any of the search terms exist in this file */
                        if(returned.filename == returnable.filename) {
                            foundInTotalList = true
                            /* update number of occurences */
                            returned.numOfOccur = (returned.numOfOccur + returnable.numOfOccur)
                        }
                    })

                    /* if the filename doesn't exist, add the whole object to our list */
                    if(foundInTotalList == false) {
                        totalList += returnable
                    }
                    foundInTotalList = false
                })
            }
            else {
                /* If its the first term, there's nothing to compare - just add the filename, occurances, and the offsets to our list */
                if(searchingForOffsets.size == 0) {
                    result.foreach(a => {
                        var pr: ParserReturnable = new ParserReturnable()
                        pr.filename = a.filename
                        pr.numOfOccur = a.numOfOccur
                        val offsets = a.offsets.asScala
                        offsets.foreach{case (offset:Integer) => {
                                /* we want to add the length of the term plus 1 for the space character because the next term must have an offset equal to this value in order to appear sequentially in the file */
                                pr.offsets.add(offset + term.length() + 1)
                                
                        }}
                        searchingForOffsets += pr
                    })
                }
                else {
                    /* Not the first term to be searched. Need to determine whether an offsets from the current term equals a term's offset from the previous term for the same file */
                    var temp: ArrayBuffer[ParserReturnable] = new ArrayBuffer[ParserReturnable]()

                    result.foreach(output => {
                        searchingForOffsets.foreach(last => {
                            if(output.filename == last.filename) {

                                /* performs intersection of the lists and puts back into 'output' variable */
                                output.offsets.retainAll(last.offsets)

                                if(output.offsets.size > 0) {
                                    /* we have a match and the words occur sequentially in output.filename */
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

        /* end timer */
        val endTime = System.currentTimeMillis

        /* print results to console and to a file if the user desired */
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

    /* function to sort whichever buffer is applicable to the run (phrasing or regular search) based on the number of occurences and print to the console */
    def printOutput(list: ArrayBuffer[ParserReturnable]) {
        val cmp = (x:ParserReturnable, y:ParserReturnable) => x.numOfOccur > y.numOfOccur
        val sorted = scala.util.Sorting.stableSort(list, cmp)
        sorted.foreach(println)
    }

    /* function to sort the buffer given and output to a file */
    def saveOutputFile(list: ArrayBuffer[ParserReturnable], args:Array[String]) {
        val outputPath:String = args(1) + "last_results"
        val fs = FileSystem.get(new Configuration())
        val output = fs.create(new Path(outputPath))
        val out = new BufferedOutputStream(output)
        val cmp = (x:ParserReturnable, y:ParserReturnable) => x.numOfOccur > y.numOfOccur
        val sorted = scala.util.Sorting.stableSort(list, cmp)
        out.write("------ Output File -----\r\n".getBytes("UTF-8"))

        sorted.foreach(returnable => {
            out.write(returnable.toString.getBytes("UTF-8"))
            out.write("\r\n".getBytes("UTF-8"))
        })

        out.write("----- Finished -----".getBytes("UTF-8"))
        out.write("\r\n".getBytes("UTF-8"))
        out.close()
    }
}

