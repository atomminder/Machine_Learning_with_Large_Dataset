/*
 * This class is counting the frequency 
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

public class NBTrainJob {

	/* Mapper */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		/*some variables */
		private Text keyOutput = new Text();
		private Text valueOutput = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			/* iterate every line, ignore blank line */
			if(value == null || value.getLength() == 0) {
				return;
			}
			String line = value.toString();
			Vector<String> elements = NBHelper.tokenizeDoc(line);
			String[] labels = NBHelper.getLabels(line);

			/* count the  total doc */
			keyOutput.set("~td");
			valueOutput.set("" + labels.length);
			context.write(keyOutput, valueOutput);
			/* count the  total word */
			keyOutput.set("~tw");
			valueOutput.set("" + (labels.length * (elements.size() - 1)));
			context.write(keyOutput, valueOutput);

			/* count the label doc & label word */
			for(int i = 0; i < labels.length; i++) {
				
				String label = labels[i];
				/* count the label doc */
				keyOutput.set("~ld=" + label);
				valueOutput.set("" + 1);
				context.write(keyOutput, valueOutput);
				/* count the label word */
				keyOutput.set("~lw=" + label);
				valueOutput.set("" + (elements.size() - 1));
				context.write(keyOutput, valueOutput);
			}

			/* send message for each word */
			Iterator<String> itr = elements.iterator();
			itr.next();
			String word, label;
			while(itr.hasNext()) {
				
				word = itr.next();
				/* iterate each*/
				for(int i = 0; i < labels.length; i++) {
					label = labels[i];
					keyOutput.set("*" + word);
					valueOutput.set(label);
					context.write(keyOutput, valueOutput);
				}
			}
		} 
	} 

	/* Reducer */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		/* some variables */
		Text valueOutput = new Text();
		Hashtable<String, Long> hashCount = new Hashtable<String, Long>();

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			String word = key.toString();
			/* Deal with global things */
			if(word.charAt(0) == '*') {
				String label;

				/* create the hashtable for counter */
				for(Text temp: values) {
					
					label = temp.toString();
					/* if there is no this key */
					if(!hashCount.containsKey(label)) 
						hashCount.put(label, 1l);
					/* if there is already this key */
					else
						hashCount.put(label, hashCount.get(label) + 1);
				}

				/* all the counters */
				String outString = "";
				Iterator<String> labels = hashCount.keySet().iterator();
				while(labels.hasNext()) {
					label = labels.next();
					outString += label + "=" + hashCount.get(label) + " ";
				}
				valueOutput.set(outString.trim());
				context.write(key, valueOutput);
			}

			/* Deal with words counters */
			else {
				
				int sum = 0;
				for(Text val: values) 
					sum += Long.parseLong(val.toString()); 
				valueOutput.set("" + sum);
				context.write(key, valueOutput);
			}
		}
	}

	/* main */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "NBtrain");

		job.setJarByClass(NBTrainJob.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(10);
	    FileInputFormat.addInputPath(job, new Path("s3n://jasonxi/10605HW4/data/train/"));
	    FileOutputFormat.setOutputPath(job, new Path("s3n://jasonxi/10605HW4/results5/train_results/"));
		job.waitForCompletion(true);
	}
}
