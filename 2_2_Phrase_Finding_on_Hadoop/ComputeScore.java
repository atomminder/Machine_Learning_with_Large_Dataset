/**
 * Compute the final score  
 */
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ComputeScore {

	/* Mapper */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String val_string = value.toString();
			String[] elements = val_string.split("\t");
			context.write(new Text(elements[0]), new Text(elements[1]));
		} 
	} 

	/* Reducer */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		/* global count things */
		double f_uni_total = 10689910135d;
		double f_bi_total = 8539421170d;
		double b_bi_total = 30589724948d;

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			/* set as 1 for smoothing*/
			double fxy = 1, bxy = 1, fx = 1, fy = 1;
			/* extract each counter */
			for(Text val: values) {
				
				String temp = val.toString();
				String[] elements = temp.split("=");
				String label = elements[0];
				double count = Double.parseDouble(elements[1]);

				/* bigram in the foreground */
				if(label.equals("fxy")) {
					if(count > fxy)
						fxy = count;
				}
				/* bigram in the background */
				else if(label.equals("bxy")) {
					if(count > bxy)
						bxy = count;
				}		
				else if(label.equals("fx")) {
					if(count > fx)
						fx = count;
				}
				else {
					if(count > fy)
						fy = count;
				}
			}
			/* probabilities */
			fxy = fxy / f_bi_total;
			bxy = bxy / b_bi_total;
			fx = fx / f_uni_total;
			fy = fy / f_uni_total;			
			/* phrase */
			double phrase = fxy * Math.log(fxy / (fx * fy));
			/* informative */
			double informative = fxy * Math.log(fxy / bxy);
			/* output */
			context.write(key, new Text("" + (phrase + informative)));
		}
	}

	/* Main */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "ComputeScore");
		job.setJarByClass(ComputeScore.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("s3n://jasonxi/10605HW5/data/PAndI.txt"));
		FileOutputFormat.setOutputPath(job, new Path("s3n://jasonxi/10605HW5/data/Score.txt"));

		job.waitForCompletion(true);
	}
}
