import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.*;
import java.sql.Timestamp;

public class D {
    public static void main(String[] args) throws Exception {
        boolean indexCompleted = false;
        Scanner scanner = new Scanner(System.in);
        int command;
        System.out.println("Please input a command:");
        System.out.println("1 = Index \t 2 = Search word(s) \t 3 = Search phrase \t 4 = Quit");
        command = scanner.nextInt();
        //while((command = scanner.nextInt()) != 4) {
            switch(command) {
                case 1:
                    indexCompleted = RunIndex(args);
                    if(!indexCompleted) {
                        System.exit(1);
                    }
                    break;
                case 2:
                    Search(0, args[0], args[1]);
                    break;
                case 3:
                    Search(1, args[0], args[1]);
                    break;
                case 4:
                    System.exit(0);
                    break;
                default:
                    System.out.println("Please enter valid input.");
            }
            
            //System.out.println("Please input a command:");
            //System.out.println("1 = Index \t 2 = Search word(s) \t 3 = Search phrase \t 4 = Quit");
        //}
        System.exit(0);
    }
/*
	public static void main(String[] args) throws Exception {
        boolean indexCompleted = false;
        
        while(true) {
            System.out.println("Please input a command:");
            System.out.println("1 = Index \t 2 = Search word(s) \t 3 = Search phrase \t 4 = Quit");
            
            Scanner scanner = new Scanner(System.in);
            String command = scanner.next();
            
            switch(command) {
                case "1":
                    indexCompleted = RunIndex(args);
                    if(!indexCompleted) {
                        System.exit(1);
                    }
                    break;
                case "2":
                    Search(0, args[0], args[1]);
                    break;
                case "3":
                    Search(1, args[0], args[1]);
                    break;
                case "4":
                    System.exit(0);
                    break;
                default:
                    System.out.println("Please enter valid input.");
            }
        }
    }*/
    
    public static boolean RunIndex(String[] args) throws Exception {
    
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(D.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(M.class);
		//job.setCombinerClass(R.class);
		job.setReducerClass(R.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(docOffsets.class);
		
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		/* This line is to accept input recursively */
		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);


		/* Delete output filepath if already exists */
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
        Timestamp startTime = new Timestamp(System.currentTimeMillis());
		//return (job.waitForCompletion(true) ? 0 : 1);
        boolean completion = job.waitForCompletion(true);
        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        
        System.out.println("start time: " + startTime.getTime());
        System.out.println("end time: " + endTime.getTime());
        System.out.println("difference: " + (endTime.getTime() - startTime.getTime()));
        
        return completion;
	}
    
    public static void Search(int searchType, String indexInputDir, String indexOutputDir) {
        String input;
        ArrayList<ParserReturnable> totalList = new ArrayList<ParserReturnable>();
        ArrayList<ParserReturnable> searchingForOffsets = new ArrayList<ParserReturnable>();
        
        if(searchType == 0) {
            /* word search */
            System.out.println("Please enter the words to search (seperated by a single space):");
        }
        else {
            /* phrase search */
            System.out.println("Please enter the phrase to search:");
        }
        Scanner scanner = new Scanner(System.in);
        input = scanner.nextLine();
        String [] inputTerms = input.split(" ");
        
        //System.out.println("input terms: " + inputTerms.toString());
        for(String term : inputTerms) {
            System.out.println("term: " + term);
            
            LineParser lp = new LineParser("hdfs://" + indexOutputDir + "/part-r-00000", term);
            try {
                lp.parse();
                ArrayList<ParserReturnable> output = lp.parserResult;
                if(searchType == 0) {
                    /* Just keep track of all the returned output */
                    for(ParserReturnable pr: output) {
                        totalList.add(pr);
                    }
                }
                else {
                    /* need check the current output to see if it matchest a file & offset from the previous term. if so, then they occur sequentially in the text */
                    if(searchingForOffsets.size() == 0) {
                        for(ParserReturnable pr: output) {
                            ParserReturnable newPr = new ParserReturnable();
                            newPr.filename = pr.filename;
                            newPr.numOfOccur = pr.numOfOccur;
                            for(Integer offset: pr.offsets) {
                                /* the next word in the sequence should occur at "term"'s offset, plus "term"'s length, plug one for the space between words */
                                newPr.offsets.add(offset + term.length() + 1);
                            }
                            searchingForOffsets.add(newPr);
                        }
                    }
                    else {
                        /* check if the offset numbers from the previous term and the current term match */
                        ArrayList<ParserReturnable> temp = new ArrayList<ParserReturnable>();
                        
                        for(ParserReturnable pr: output) {
                            for(ParserReturnable last : searchingForOffsets) {
                                if(pr.filename.equals(last.filename)) {
                                    pr.offsets.retainAll(last.offsets);
                                    System.out.println("pr offsets: " + pr.offsets);
                                    if(pr.offsets.size() > 0) {
                                        temp.add(new ParserReturnable(last.filename, pr.offsets.size(), pr.offsets));
                                    }
                                }
                            }
                        }
                        searchingForOffsets = new ArrayList<ParserReturnable>(temp);
                    }
                }
            }
            catch (Exception e) {
                System.out.println("Error in parse: " + e.getMessage());
            }
        }
        scanner.close();
        
        if(searchType == 0) {
            for(ParserReturnable pr: totalList) {
                System.out.println(pr.toString());
            }
        }
        else {
        	for(ParserReturnable pr: searchingForOffsets) {
                	System.out.println(pr.toString());
                }
	}
    }
}
