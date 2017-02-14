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
import java.util.Iterator;

/**
 * Created by zhangshiqiu on 2017/2/14.
 */
public class DataDividerByUser {
    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: userID, movieID, rating
            String[] user_movie_rating = value.toString().trim().split(",");
            if (user_movie_rating.length < 3) {
                return;
            }
            int userID = Integer.parseInt(user_movie_rating[0]);
            String moveID = user_movie_rating[1];
            String rating = user_movie_rating[2];
            context.write(new IntWritable(userID), new Text(moveID + ":" + rating));
        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key: userID
            // values: <moveID:rating, ...>

            // output key: userID
            // output value: list of movies
            StringBuilder builder = new StringBuilder();
            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()){
                builder.append(",").append(iter.next().toString());
            }
            context.write(key, new Text(builder.toString().replaceFirst(",", "")));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        job.setJarByClass(DataDividerByUser.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
