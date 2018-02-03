import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatrixTransposeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // col  list(row_num, row_num)
        // 1  [ 2_3, 4_1, 3_2, ...    ]
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString()).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        context.write(key, new Text(builder.toString()));
    }
}
