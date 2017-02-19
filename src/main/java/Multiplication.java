import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
/**
 * Created by zhangshiqiu on 2017/2/17.
 */
public class Multiplication {
    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: movieB \t movieA=relation
            String[] line = value.toString().trim().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input: user,movie,rating
            //pass data to reducer
            String[] line = value.toString().trim().split(",");
            if (line.length != 3){
                return;
            }
            context.write(new Text(line[1]), new Text(line[0]+":"+line[2]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = movieB
            // value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
            //collect the data for each movie, then do the multiplication

            Map<String, Double> relationMap = new HashMap<>();
            Map<String, Double> ratingMap = new HashMap<>();
            for (Text value : values) {
                String val = value.toString().trim();
                if (val.contains("=")){
                    String[] movie_relation = val.split("=");
                    relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                } else {
                    String[] user_rating = val.split(":");
                    ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            StringBuffer stringBuffer = new StringBuffer();
            for (Map.Entry<String, Double> relationEntry : relationMap.entrySet()) {
                String movieA = relationEntry.getKey();
                double relation = relationEntry.getValue();
                for (Map.Entry<String, Double> ratingEntry : ratingMap.entrySet()) {
                    String user = ratingEntry.getKey();
                    double rating = ratingEntry.getValue();
                    stringBuffer.append(user).append(":").append(movieA);
                    String outputKey = stringBuffer.toString();
                    stringBuffer.setLength(0);

                    double outputValue = relation * rating;
                    context.write(new Text(outputKey), new DoubleWritable(outputValue));
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(CooccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
