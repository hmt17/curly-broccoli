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
