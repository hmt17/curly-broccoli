import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class R extends Reducer<Text, docOffsets, NullWritable, Text> {

	@Override
	public void reduce(final Text key, final Iterable<docOffsets> values,
			final Context context) throws IOException, InterruptedException {

        /* HashMap to represent a word with value that is also a HashMap representing the offsets for each file name */
		HashMap<String, HashMap<String, ArrayList<String>>> m = new HashMap<String, HashMap<String, ArrayList<String>>>();
        
		for (docOffsets value : values) {
            
            if(m.containsKey(key.toString())){
                HashMap<String, ArrayList<String>> current = m.get(key.toString());
                
                if(current.containsKey(value.filename.toString())) {
                    ArrayList list = (ArrayList) current.get(value.filename.toString());
                    list.add(value.offset.toString());
                    current.put(value.filename.toString(), list);
                }
                else {
                    current.put(value.filename.toString(), new ArrayList<String>(Arrays.asList(value.offset.toString())));
                }
                
            }
            else {
                HashMap<String, ArrayList<String>> newHm = new HashMap<String, ArrayList<String>>();
                newHm.put(value.filename.toString(), new ArrayList<String>(Arrays.asList(value.offset.toString())));
                m.put(key.toString(), newHm);
            }
        }
        
        
        for(String words: m.keySet()){
            StringBuilder sb = new StringBuilder("");
            HashMap<String, ArrayList<String>> current = m.get(words);
            sb.append(words);
            sb.append("-");
            
            for(String filename : current.keySet()) {
                ArrayList<String> offsets = current.get(filename);
                sb.append("{[" + filename + "]");
                sb.append("[" + offsets.size() + "]");
                sb.append("[");
                int count = 0;
                while(count < offsets.size()) {
                    sb.append(offsets.get(count));
                    count++;
                    if(count < offsets.size()) {
                        sb.append(',');
                    }
                }
                sb.append("]}");
            }
            
            /* write a NullWritable in place of the key because we have already put the word in the sb for proper parsing */
            context.write(NullWritable.get(),new Text( sb.toString()));
        }
    }
}
