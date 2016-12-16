package com.cloud;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.*;
import java.io.*;
import java.sql.Timestamp;

public class D {
    public static void main(String[] args) throws Exception {
        boolean indexCompleted = false;
        Scanner scanner = new Scanner(System.in);
        int command;
        boolean exit = false;
        //System.out.println("Please input a command:");
        //System.out.println("1 = Index \t 2 = Search word(s) \t 3 = Search phrase \t 4 = Quit");
        //command = scanner.nextInt();
        //while((command = scanner.nextInt()) != 4) {
        //while(exit == false){
        System.out.println("Please input a command:");
        System.out.println("1 = Index \t 2 = Search word(s) \t 3 = Search phrase \t \t 4 = Quit");
        command = scanner.nextInt();
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
                exit = true;
                break;
            default:
                System.out.println("Please enter valid input.");
        }
        //}
        System.exit(0);
    }
    
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
        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        String [] inputTerms = input.split(" ");
        
        //System.out.println("input terms: " + inputTerms.toString());
        for(String term : inputTerms) {
            System.out.println("term: " + term);
            
            LineParser lp = new LineParser("hdfs://" + indexOutputDir + "/part-r-00000", term);
            try {
                lp.parse();
                ArrayList<ParserReturnable> output = lp.parserResult;
                if(searchType == 0) {
                    /* Just keep track of all the returned output and put in descending order */
                    boolean found = false;
                    for(ParserReturnable pr: output) {
                        for(ParserReturnable pr2: totalList){
                            if(pr2.filename.equals(pr.filename)){
                                found = true;
                                pr2.numOfOccur = pr2.numOfOccur + pr.numOfOccur;
                            }
                        }
                        if(!found){
                            totalList.add(pr);
                        }
                        found = false;
                    }
                }
                else {
                    /* need check the current output to see if it matchest a file & offset from the previous term. 
                     if so, then they occur sequentially in the text */
                    if(searchingForOffsets.size() == 0) {
                        for(ParserReturnable pr: output) {
                            ParserReturnable newPr = new ParserReturnable();
                            newPr.filename = pr.filename;
                            newPr.numOfOccur = pr.numOfOccur;
                            for(Integer offset: pr.offsets) {
                                /* the next word in the sequence should occur at "term"'s offset, plus "term"'s length, 
                                 plug one for the space between words */
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
        
        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        
        //ArrayList<ParserReturnable> newList = new ArrayList<ParserReturnable>();
        System.out.println("Would you like your results saved? Press 1 for yes or 2 for no: ");
        int saved = scanner.nextInt();
        int j=0;
        if(searchType == 0) {
            printResults(totalList, indexOutputDir, saved);
        }
        else {
            printResults(searchingForOffsets, indexOutputDir, saved);
        }
        scanner.close();
        System.out.println("start time: " + startTime.getTime());
        System.out.println("end time: " + endTime.getTime());
        System.out.println("difference: " + (endTime.getTime() - startTime.getTime()));
    }
    
    public static void printResults(ArrayList<ParserReturnable> list, String outputDir, int saved) {
        Collections.sort(list, new Comparator<ParserReturnable>() {
            @Override
            public int compare(ParserReturnable p1, ParserReturnable p2){
                
                if( p1.numOfOccur > (p2.numOfOccur)){
                    return -1; 
                }
                else if( p1.numOfOccur < (p2.numOfOccur)){
                    return 1; 
                }
                else{
                    return 0;
                }
            }
        });
        
        if(list.size() > 0) {
            for(ParserReturnable pr: list){
                System.out.println(pr.toString());
            }
        }
        else {
            System.out.println("No results found.");
        }
        
        if(saved==1){
            //FileWriter fw = new FileWriter ("textfile.txt");
            // BufferedWriter writer = (fw);
            try {
                FileSystem fileSystem = FileSystem.get(new Configuration());
                FSDataOutputStream stm = fileSystem.create(new Path("hdfs://" + outputDir +"/output.txt"));
                String line = "";
                
                if(list.size() > 0) {
                    for(ParserReturnable pr: list){
                        line = pr.toString();
                        stm.writeBytes(line);
                        stm.writeBytes("\n");
                    }
                }
                else {
                    stm.writeBytes("No results found.");
                }
                stm.close();
            }
            catch (Exception e) {
                System.out.println("Error writing output file: " + e);
            }
        }
    }
    
}
