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
		 filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		//filename = new Text(filenameStr);
		
		String line = value.toString();

		line = line.toLowerCase();
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			//word.set(tokenizer.nextToken());			
			String words = tokenizer.nextToken();
			if(!hm.containsKey(words)){
 
 				//count=(int)m.get(str);
				 
				hm.put(words, new ArrayList<Long>(Arrays.asList(offset)));
				 
			}else{
				ArrayList list = (ArrayList) hm.get(words);
				list.add(offset);
				hm.put(words, list);
			}
			offset += words.length()+1;
		}
	}
	public void configure(JobConf job) {
		
		offset = job.getLong("map.input.start", 0);
		
	}
	public void cleanup(Context context)throws IOException, InterruptedException {
		
		for(String words: hm.keySet()){
			ArrayList<Long> list = hm.get(words);
			for(long offset: list){
				context.write(new Text(words), new docOffsets(filename, offset));
			}
		}
	}
}
