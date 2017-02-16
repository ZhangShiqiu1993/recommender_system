import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by zhangshiqiu on 2017/2/16.
 */
public class CoOccurrenceMatrixGenerator {
    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //value = userid\tmovie1:rating,movie2:rating...
            String[] user_movieRating = value.toString().trim().split("\t");
            if (user_movieRating.length != 2) {
                return;
            }

            String[] movie_ratings = user_movieRating[1].split(",");
            for (int i = 0; i < movie_ratings.length; i++) {
                String movieA = movie_ratings[i].trim().split(":")[0];
                for (int j = 0; j < movie_ratings.length; j++) {
                    String movieB = movie_ratings[j].trim().split(":")[0];
                    context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
                }
            }
        }
    }

    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable value : values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
