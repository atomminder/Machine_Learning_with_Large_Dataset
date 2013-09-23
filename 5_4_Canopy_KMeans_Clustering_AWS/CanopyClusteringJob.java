import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class CanopyClusteringJob {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {



		// Count the iteration 
		//int iteration = 1;
		//conf.set("num.interation", iteration + "");

		//**************************************************************
		// Job1
		Configuration conf1 = new Configuration();

		Path in1 = new Path("s3n://jasonxi/10605HW7/canopyclustering/import/initialData/");
		Path canopy = new Path("s3n://jasonxi/10605HW7/canopyclustering/import/canopy/Canopy.txt");
		Path out1 = new Path("s3n://jasonxi/10605HW7/canopyclustering/step1");
		conf1.set("canopy.path", canopy.toString());

		Job job1 = new Job(conf1);
		job1.setJobName("Find Canopies");
		job1.setMapperClass(CanopySelMapper.class);
		//job.setCombinerClass(KMeansCombiner.class);
		job1.setReducerClass(CanopySelReducer.class);
		job1.setJarByClass(CanopySelMapper.class);

		FileInputFormat.addInputPath(job1, in1);
		FileSystem fs1 = in1.getFileSystem(conf1);
		//if (fs1.exists(out1))
		//	fs1.delete(out1, true);
		FileOutputFormat.setOutputPath(job1, out1);	

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);	

		job1.waitForCompletion(true);

		//**************************************************
		//job2
		Configuration conf2 = new Configuration();

		Path in2 = new Path("s3n://jasonxi/10605HW7/canopyclustering/import/initialData/");
		//Path canopy = new Path("files/clustering/import/canopy/Canopy.txt");
		Path out2 = new Path("s3n://jasonxi/10605HW7/canopyclustering/step2");
		conf2.set("canopy.path", canopy.toString());

		Job job2 = new Job(conf2);
		job2.setJobName("Assign Canopies");
		job2.setMapperClass(AssignCanopy.class);
		//job.setCombinerClass(KMeansCombiner.class);
		//job2.setReducerClass(CanopySelReducer.class);
		job2.setJarByClass(AssignCanopy.class);

		FileInputFormat.addInputPath(job2, in2);
		FileSystem fs2 = in2.getFileSystem(conf2);
		//if (fs2.exists(out2))
		//	fs2.delete(out2, true);
		FileOutputFormat.setOutputPath(job2, out2);	

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);	

		job2.waitForCompletion(true);

		//**************************************************
		//job3 and more

		// Count the iteration 
		int iteration = 1;


		Configuration conf = new Configuration();
		conf.set("num.interation", iteration + "");
		FileSystem fs = FileSystem.get(conf);
		Path in = new Path("s3n://jasonxi/10605HW7/canopyclustering/step2/");
		Path center = new Path("s3n://jasonxi/10605HW7/canopyclustering/import/center/center.txt");
		//if (fs.exists(center))
		//	fs.delete(center, true);
		Path out = new Path("s3n://jasonxi/10605HW7/canopyclustering/step3/depth_1");
		canopy = new Path("s3n://jasonxi/10605HW7/canopyclustering/import/canopy/Canopy.txt");
		conf.set("centroids.path", center.toString());
		conf.set("canopy.path", canopy.toString());

		
		Job job = new Job(conf);
		job.setJobName("KMeans Culstering");

		job.setMapperClass(KMeansMapper.class);
		job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);


		FileInputFormat.addInputPath(job, in);

		//if (fs.exists(out))
		//	fs.delete(out, true);
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
			conf.set("centroids.path", center.toString());
			conf.set("canopy.path", canopy.toString());
			conf.set("num.iteration", iteration + "");
			job = new Job(conf);
			job.setJobName("KMeans Clustering " + iteration);

			job.setMapperClass(KMeansMapper.class);
			job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);
			//job.setJarByClass(KMeansCombiner.class);
			//job.setJarByClass(KMeansReducer.class);

			in = new Path("s3n://jasonxi/10605HW7/canopyclustering/step2/");
			center = new Path("s3n://jasonxi/10605HW7/canopyclustering/import/center/center.txt");
			out = new Path("s3n://jasonxi/10605HW7/canopyclustering/step3/depth_" + iteration);

			FileInputFormat.addInputPath(job, in);
			//if (fs.exists(out))
			//	fs.delete(out, true);
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
