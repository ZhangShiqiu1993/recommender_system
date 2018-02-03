import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class ProductReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		TreeMap<String, String> map = new TreeMap<String, String>();

		for (Text value : values) {
			String[] col_num = value.toString().split("_");
			map.put(col_num[0], col_num[1]);
		}

		Iterator<String> iterator = map.keySet().iterator();
		while (iterator.hasNext()) {
			String col = iterator.next();
			builder.append(col).append("_").append(map.get(col)).append(",");
		}
		builder.deleteCharAt(builder.length() - 1);
		context.write(key, new Text(builder.toString()));
	}
}
