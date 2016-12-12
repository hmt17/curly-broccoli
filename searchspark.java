import scala.io.StdIn
import scala.util.control._
import java.io.File
import scala.reflect._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import java.util.*;

public class searchspark{
	public static void main(String[] args){
		Scanner scanner = new Scanner(System. in); 
		String input = scanner. nextLine();
		String[] cmdArray = new String[2];
		if(input == 1 || input == 2){
		//run normal search or phrasing 
			cmdArray[0] = "cloud.scala";
			cmdArray[1] = "temp.txt";
			Process process = Runtime.getRuntime().exec(cmdArray,null);
			//Runtime runtime = Runtime.getRuntime();
		    	//Process process = runtime.exec("myprocess");
		}
		if(input ==3){
		//save output

		}
		if(input ==3){
		//exit
			System.exit(0);
		}

	}
}


/**
Some links that I've been looking at - i think java is better but a few people are using all python for this

python:
https://pymotw.com/2/subprocess/
http://stackoverflow.com/questions/40027207/python-subprocess-module-hangs-for-spark-submit-command-when-writing-stdout
http://stackoverflow.com/questions/16768290/understanding-popen-communicate

java:
https://stackoverflow.com/questions/26817940/how-to-use-map-function-in-spark-with-java
http://stackoverflow.com/questions/11352037/scala-utc-timestamp-in-seconds-since-january-1st-1970
https://www.tutorialspoint.com/java/lang/runtime_exec_envp.htm
http://alvinalexander.com/scala/scala-import-java-classes-packages-examples
https://stackoverflow.com/questions/33265056/how-do-i-determine-an-offset-in-apache-spark#33321491
https://stackoverflow.com/questions/33265056/how-do-i-determine-an-offset-in-apache-spark
**/
