import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class KMeansClusteringJob {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {



		// Count the iteration 
		int iteration = 1;
		
		
		Configuration conf = new Configuration();
		conf.set("num.interation", iteration + "");

		Path in = new Path("files/clustering/import/USCensus1990.full.txt");
		Path center = new Path("files/clustering/import/center/centroids12.full.txt");
		Path out = new Path("files/clustering/depth_1");
		conf.set("centroid.path", center.toString());

		Job job = new Job(conf);
		job.setJobName("KMeans Culstering");

		job.setMapperClass(KMeansMapper.class);
		job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);
		//job.setJarByClass(KMeansCombiner.class);
		//job.setJarByClass(KMeansReducer.class);

		FileInputFormat.addInputPath(job, in);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out))
			fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);	

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);	

		job.waitForCompletion(true);

		long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();

		++iteration;

		// if not converged
		while(counter > 0) {
			
			// Output the iteration time
			System.out.println("Iteration:" + iteration);

			
			conf = new Configuration();
			conf.set("centroid.path", center.toString());
			conf.set("num.iteration", iteration + "");
			job = new Job(conf);
			job.setJobName("KMeans Clustering " + iteration);

			job.setMapperClass(KMeansMapper.class);
			job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);
			//job.setJarByClass(KMeansCombiner.class);
			//job.setJarByClass(KMeansReducer.class);

			in = new Path("files/clustering/import/USCensus1990.full.txt");
			out = new Path("files/clustering/depth_" + iteration);

			FileInputFormat.addInputPath(job, in);
			if (fs.exists(out))
				fs.delete(out, true);
			FileOutputFormat.setOutputPath(job, out);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			job.waitForCompletion(true);
			
			++iteration;
			counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();

		}
		System.out.println("Count:" + (iteration-1));
	}
}
