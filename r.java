import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class R extends Reducer<Text, docOffsets, Text, Text> {

	@Override
	public void reduce(final Text key, final Iterable<docOffsets> values,
			final Context context) throws IOException, InterruptedException {

		// HashMap<String, Integer> m=new HashMap<String, Integer>();
		// StringBuilder stringBuilder = new StringBuilder();
		// Iterable<Map.Entry<String, Integer>> keySet;
		// int count =0;
		
		// for (Text value : values) {
			// String str = value.toString();

			// if(m.containsKey(str)){
 
 				// //count=(int)m.get(str);
				 
				// m.put(str, m.get(str)+1);
				 
			// }else{
				// m.put(str, 1);
			// }
		// }
		// keySet = m.entrySet();
		// //keySet = m.entrySet().iterator();
		// //java.util.Iterator keySet = m.keySet().iterator();
		// for(Map.Entry<String, Integer> keys: keySet){
			// stringBuilder.append(keys.getKey() + "\\t" + keys.getValue());
			// if (keySet.iterator().hasNext()) {
				// stringBuilder.append("\\t");
			// }
		// }
		for (docOffsets value : values) {
			String str2 = value.filename + " " + value.offset;
		context.write(key,new Text( str2.toString()));
		 //new Text(stringBuilder.toString()));
		}
	}

}
