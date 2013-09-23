/*
 * This class is merging the count
 * andrew id: qxi
 * name: Qiangjian Xi 
 * 
 */
import java.io.IOException;
import java.util.*;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NBMergeJob {

	/* Mapper */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		/* some variables */
		private Text keyOutput = new Text();
		private Text valueOutput = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			/* if this is the line in the test file  */
			if(line.charAt(0) != '~' && line.charAt(0) != '*') {

				valueOutput.set(key.toString());
				Vector<String> elements = NBHelper.tokenizeDoc(line);
				Iterator<String> itr = elements.iterator();
				/* iterate each */
				itr.next();
				while(itr.hasNext()) {

					keyOutput.set("*" + itr.next());
					context.write(keyOutput, valueOutput);
				}
				/* Deal with global messages */
				keyOutput.set("~td");
				context.write(keyOutput, valueOutput);
				keyOutput.set("~tw");
				context.write(keyOutput, valueOutput);
				String[] labels = NBHelper.getLabels(line);
				String label;
				for(int i = 0; i < labels.length; i++) {
					label = labels[i];
					keyOutput.set("~ld=" + label);
					context.write(keyOutput, valueOutput);
					keyOutput.set("~lw=" + label);
					context.write(keyOutput, valueOutput);
				}
			}

			/* if it is the line in the count file */
			else {

				String[] elements = line.split("\t");
				keyOutput.set(elements[0]);
				if(elements[0].charAt(0) == '*') {
					valueOutput.set(elements[1]);
				}
				else {
					valueOutput.set("ct=" + elements[1]);
				}
				context.write(keyOutput, valueOutput);
			}    
		}
	} 

	/* Combiner */
	public static class Combine extends Reducer<Text, Text, Text, Text> {

		/* some variables */
		private Text valueOutput = new Text();

		public void combine(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			StringBuffer out = new StringBuffer();
			String line ;

			for (Text val : values) {

				line = val.toString();
				out = new StringBuffer();
				/* deal with message */
				if(Character.isDigit(line.charAt(0))) 
					out.append(line + " ");	    			   
				/* deal with counter */
				else 
					context.write(key, val);	
			}

			/* Combine the doc ids */
			valueOutput.set(out.toString());
			context.write(key, valueOutput);
		}

		/* reducer */
		public static class Reduce extends Reducer<Text, Text, Text, Text> {

			/* some variables */
			private Hashtable<String, String> docIds = new Hashtable<String, String>();

			public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {

				Text counter = null;
				docIds.clear();

				/* iterate */
				for(Text temp: values) {

					/*  deal with message */
					if(Character.isDigit(temp.toString().charAt(0))) {
						String id = temp.toString();
						if(!docIds.containsKey(id))
							docIds.put(id, "");
					}
					/* deal with counter */
					else {
						counter = new Text(temp.toString());
					}
				}
				if(counter != null && !docIds.isEmpty()) {

					StringBuffer sb = new StringBuffer();
					sb.delete(0, sb.length());
					sb.append("~" + key.toString() + "\t" + counter.toString());
					Text outKey = new Text();
					Text valueOutput = new Text(sb.toString());
					/* iterate each */
					Iterator<String> itrs = docIds.keySet().iterator();
					while(itrs.hasNext()) {
						outKey = new Text(itrs.next());
						context.write(outKey, valueOutput);
					}
				}

			}
		}

		/* main */
		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration(); 
			Job job = new Job(conf, "NBMerge");

			job.setJarByClass(NBMergeJob.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setCombinerClass(Combine.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setNumReduceTasks(10);
			FileInputFormat.addInputPath(job, new Path("s3n://jasonxi/10605HW4/data/test/"));
			FileInputFormat.addInputPath(job, new Path("s3n://jasonxi/10605HW4/results5/train_results/"));
			FileOutputFormat.setOutputPath(job, new Path("s3n://jasonxi/10605HW4/results5/merge_results/"));

			job.waitForCompletion(true);
		}

	}
}
