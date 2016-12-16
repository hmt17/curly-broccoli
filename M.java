package com.cloud;

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class M extends
		Mapper<LongWritable, Text, Text, docOffsets> {

	private Text word = new Text();
	private String filename;
	long offset;
	HashMap<String, ArrayList<Long>> hm = new HashMap<String, ArrayList<Long>>();
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
                
        /* gets the filename we're mapping in this node */
		 filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		String line = value.toString();

        /* set everything to lowercase for consistency in searching */
		line = line.toLowerCase();
		
        /* traverse all words in this line */
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			String words = tokenizer.nextToken();
            
			if(!hm.containsKey(words)){
                /* put word in HashMap with this offset */
				hm.put(words, new ArrayList<Long>(Arrays.asList(offset)));
			}
            else {
                /* add this offset to the existing word */
				ArrayList list = (ArrayList) hm.get(words);
				list.add(offset);
				hm.put(words, list);
			}
            
            /* offset should increase the length of this word as well as 1 for the space character */
			offset += words.length()+1;
		}
	}
	public void configure(JobConf job) {
		/* get offset of this line in the file */
		offset = job.getLong("map.input.start", 0);
		
	}
	public void cleanup(Context context)throws IOException, InterruptedException {
		/* write out the words and their offset */
		for(String words: hm.keySet()){
			ArrayList<Long> list = hm.get(words);
			for(long offset: list){
				context.write(new Text(words), new docOffsets(filename, offset));
			}
		}
	}
}
