import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class MatrixReaderMapper extends Mapper<Object, Text, Text, Text> {
	private ArrayList<String> cacheList = new ArrayList<String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileReader fileReader = new FileReader("matrix2");
		BufferedReader reader = new BufferedReader(fileReader);
		String line;
		while ((line = reader.readLine()) != null) {
			cacheList.add(line);
		}
		fileReader.close();
		reader.close();
		// 1 \t 1_2,2_3,3_4
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// 1 \t 1_2,2_3,3_4
		// lines in left matrix
		String[] rowNums1 = value.toString().trim().split("\\s+");
		String row1 = rowNums1[0];
		String[] colNums1 = rowNums1[1].split(",");  // col_num

		for (String line : cacheList) {
			// lines in right matrix
			String[] colNums2 = line.split("\\s+");
			String col2 = colNums2[0];
			String[] rowNums2 = colNums2[1].split(",");

			int result = 0;
			for (String colNum1 : colNums1) {

				String[] col1_num = colNum1.split("_");
				String col1 = col1_num[0];
				int num1 = Integer.parseInt(col1_num[1]);

				for (String rowNum2 : rowNums2) {
					String[] row2_num = rowNum2.split("_");
					String row2 = row2_num[0];

					if (col1.equals(row2)) {
						int num2 = Integer.parseInt(row2_num[1]);
						result += num1 * num2;
						break;
					}
				}
			}
			context.write(new Text(row1), new Text(col2 + "_" + result));
		}
	}
}

