/*
 * Predicting 
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

public class NBPredictJob {


	/* mapper */
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			/* ignore the blank line  */
			if(value == null || value.toString().length() == 0)
				return;

			String line = value.toString();
			/* deal with the counter */
			if(Character.isDigit(value.toString().charAt(0))) {
				long index = Long.parseLong(line.substring(0, line.indexOf("\t")));
				String counter = line.substring(line.indexOf("\t") + 1);
				context.write(new LongWritable(index), new Text(counter));
			}
			/* deal with the line of test file */
			else
				context.write(key, value);
		}
	} 

	/* reducer */
	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			/* some varibles */
			Vector<String> elements = null;
			String[] labels = null;
			Hashtable<String, Long> C = new Hashtable<String, Long>();
			String line;
			String label;
			long counter;
			/* iterate */
			for(Text val: values) {
				
				line = val.toString();
				/* if this is the doc */
				if(line.charAt(0) != '~') {
					elements = NBHelper.tokenizeDoc(line);
					labels = NBHelper.getLabels(line);
				}

				/* deal with the global things */ 
				else if(line.charAt(1) == '~'){
					String[] parts = line.split("\t");
					String word = parts[0];
					counter = Long.parseLong(parts[1].substring(3));
					C.put(word, counter);
				}

				/* deal with the normal counter */
				else {
					String[] parts = line.split("\t");
					String word = parts[0].substring(2);
					String[] counters = parts[1].trim().split(" ");
					for(int i = 0; i < counters.length; i++) {
						label =  counters[i].substring(0, counters[i].indexOf("="));
						counter = Long.parseLong(
								counters[i].substring(counters[i].indexOf("=") + 1));
						C.put(word + "=" + label, counter);
					}
				}
			}
			double td = C.get("~~td");
			double tw = C.get("~~tw");
			
			/* find the max log & label*/
			String labelMax = "";
			double logMax = 10 * (long)Integer.MIN_VALUE;
			/* iterate */
			for(int i = 0; i < labels.length; i++) {
				double logCurrent = 1;
				label = labels[i];
				double ld = C.get("~~ld=" + label);
				double lw = C.get("~~lw=" + label);

				Iterator<String> itr = elements.iterator();
				itr.next();
				while(itr.hasNext()) {

					String word = itr.next();
					String cKey = word + "=" + label;
					if(C.containsKey(cKey)) {
						long wdCount = C.get(cKey);
						logCurrent = logCurrent * Math.log((wdCount + 1) / (2 * lw));	    		
					}
					else {
						logCurrent = logCurrent * Math.log(1 / (2 * lw));	    
					}
				}
				logCurrent = logCurrent + Math.log(ld / td);
				/* compare */
				if(logCurrent > logMax) {
					logMax = logCurrent;
					labelMax = label;
				}
			}

			/* deal with the result */
			StringBuffer sb = new StringBuffer();
			sb.append(elements.iterator().next() + "\t");
			sb.append(labelMax + "\t");
			sb.append(logMax + '\t');
			boolean flag = false;
			for(int i = 0; i < labels.length; i++) {
				
				if(labels[i].equals(labelMax))
					flag = true;
			}
			if(flag)
				sb.append("h");
			else
				sb.append("m");
			context.write(key, new Text(sb.toString()));

		}
	}

	/* main */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();	        
		Job job = new Job(conf, "classifier");

		job.setJarByClass(NBPredictJob.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job, new Path("s3n://jasonxi/10605HW4/results5/merge_results/"));
		FileInputFormat.addInputPath(job, new Path("s3n://jasonxi/10605HW4/data/test/"));
		FileOutputFormat.setOutputPath(job, new Path("s3n://jasonxi/10605HW4/results5/predict_results"));

		job.waitForCompletion(true);
	}
}
