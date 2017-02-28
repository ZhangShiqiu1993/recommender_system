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

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			//do nothing, just pass data to reducer
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
            // output key: movieID
            // output value: user:rating
            String[] user_movingRatings = value.toString().trim().split(",");
            StringBuilder builder = new StringBuilder();
            builder.append(user_movingRatings[0]).append(":").append(user_movingRatings[2]);
            context.write(new Text(user_movingRatings[1]), new Text(builder.toString()));
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
            Map<String, Double> movieRelation = new HashMap<String, Double>();
            Map<String, Double> userRating = new HashMap<String, Double>();
            for (Text value : values) {
                String val = value.toString().trim();
                if (val.contains("=")){
                    String[] movie_relation = val.split("=");
                    movieRelation.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                } else if (val.contains(":")){
                    String[] user_rating = val.split(":");
                    userRating.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, Double> movie_relation : movieRelation.entrySet()) {
                String movieA = movie_relation.getKey();
                double relation = movie_relation.getValue();
                for (Map.Entry<String, Double> user_rating : userRating.entrySet()) {
                    String user = user_rating.getKey();
                    double rating = user_rating.getValue();
                    builder.append(user).append(":").append(movieA);
                    context.write(new Text(builder.toString()), new DoubleWritable(relation * rating));
                    builder.setLength(0);
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
