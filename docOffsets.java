package com.cloud;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import java.io.*;
public class docOffsets implements Writable{
	
		Text filename;
		LongWritable offset;
    
		public docOffsets()
		{
				this.filename= new Text();
				this.offset = new LongWritable(0);
		}
		public docOffsets(String filename, long offset)
		{
			 this.filename = new Text(filename);
			this.offset = new LongWritable(offset);
		}
		@Override
		public void write(DataOutput out) throws IOException{
			filename.write(out);
			offset.write(out);
		}
		@Override 
		public void readFields(DataInput in) throws IOException{
			filename.readFields(in);
			offset.readFields(in);
		}
    
}
