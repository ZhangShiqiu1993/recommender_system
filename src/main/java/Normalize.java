import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by zhangshiqiu on 2017/2/16.
 */

public class Normalize {
    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // value: movieA : movieB \t relation
            String[] movie_realtion = value.toString().trim().split("\t");
            if (movie_realtion.length != 2){
                return;
            }
            String[] movies = movie_realtion[0].split(":");
            String movieA = movies[0], movieB = movies[1];
            context.write(new Text(movieA), new Text(movieB + "=" + movie_realtion[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // sum --> denominator
            // context write transpose
            // key = movieA value = {movie2=10, movie3=30, movie4=15}

            int sum = 0;
            Map<String, Integer> map = new HashMap<>();
            for (Text value : values) {
                String[] movie_relation = value.toString().split("=");
                int relation = Integer.parseInt(movie_relation[1].trim());
                sum += relation;
                map.put(movie_relation[0], relation);
            }

            for (Map.Entry<String, Integer> entry : map.entrySet()){
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + ((double) entry.getValue() / sum);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
