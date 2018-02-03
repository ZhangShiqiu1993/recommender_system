import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixTransposeMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //  1   1_1,2_3
        // row  col_num, col_num
        String[] rowNumbers = value.toString().split("\\s+");
        String row = rowNumbers[0];
        String[] numbers = rowNumbers[1].split(",");

        for (String number : numbers) {
            String[] colNum = number.split("_");
            String col = colNum[0];
            String num = colNum[1];

            context.write(new Text(col), new Text(row + "_" + num));
        }
        // col   row_num
    }
}