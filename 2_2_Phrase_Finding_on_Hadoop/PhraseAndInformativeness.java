/**
 * Count the needed things for the calculation of informativeness and phraseness 
 * 
 * Input file is unigram_full.txt & bigram_full.txt
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

public class PhraseAndInformativeness {

	/* Mapper */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			/* if the input is not valid, just return  */
			if(value == null || value.toString().length() == 0)
				return;
			/* extract the information in a line */
			String line = value.toString();
			String[] elements = line.split("\t");
			String term = elements[0];
			String year = elements[1];
			String count = elements[2];
			/* readin is a unigram */
			if(term.indexOf(" ") < 0) {
				if(year.equals("1960"))
					context.write(new Text(term), new Text("ct=" + count));
			}
			/* readin is a bigram */
			else {

				/* send message */
				String[] bigram = term.split(" ");
				context.write(new Text(bigram[0]), value);
				context.write(new Text(bigram[1]), value);
			}
		} 
	} 

	/* Reducer */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			String line = key.toString();
			long wordCount = 0;
			/* Use hashtable to store the things */
			Hashtable<String, long[]> front  = new Hashtable<String, long[]>();
			Hashtable<String, String> back = new Hashtable<String, String>();
			/* iterate all the elements */
			for(Text val: values) {

				String valurStr = val.toString();
				/* it is the count things */
				if(valurStr.indexOf("ct=") >= 0) 
					wordCount = Long.parseLong(valurStr.substring(valurStr.indexOf("ct=") + 3));		
				/* if read a message */
				else {

					String[] term = valurStr.split("\t");
					String bigram = term[0];
					String[] grams = bigram.split(" ");
					String year = term[1];
					String count = term[2];
					/* for the first word in the gram */
					if(grams[0].equals(line)) {

						/*the bigram does not exist */
						if(!front.containsKey(bigram)) {
							long[] counts = new long[2];

							/* readin is a fg */
							if(year.equals("1960")) {

								counts[0] = Long.parseLong(count);
								counts[1] = 0;
							}
							/* readin is a bg */
							else {

								counts[0] = 0;
								counts[1] = Long.parseLong(count);
							}
							front.put(bigram, counts);
						}
						/*the bigram exists */
						else {

							long[] counts = front.get(bigram);
							/* readin is a fg */
							if(year.equals("1960"))
								counts[0] = Long.parseLong(count);
							/* readin is a bg */
							else
								counts[1] += Long.parseLong(count);
						}
					}
					/* for the second word in the gram */
					else {

						/*the bigram does not exist */
						if(!back.containsKey(bigram)) {
							back.put(bigram, "");
						}
					}
				}
			}
			/* output the count things here */
			Iterator<String> itr;
			itr = front.keySet().iterator();
			while(itr.hasNext()) {
				
				String bigram = itr.next();
				long[] count = front.get(bigram);
				context.write(new Text(bigram), new Text("fx=" + wordCount));
				context.write(new Text(bigram), new Text("fxy=" + count[0]));
				context.write(new Text(bigram), new Text("bxy=" + count[1]));
			}
			itr = back.keySet().iterator();
			while(itr.hasNext()) {
				
				String bigram=  itr.next();
				context.write(new Text(bigram), new Text("fy=" + wordCount));
			}	
		}
	}

	/* Main */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "PhraseAndInformativeness");
		job.setJarByClass(PhraseAndInformativeness.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("s3n://jasonxi/10605HW5/data/unigram_full.txt"));
		FileInputFormat.addInputPath(job, new Path("s3n://jasonxi/10605HW5/data/bigram_full.txt"));
		FileOutputFormat.setOutputPath(job, new Path("s3n://jasonxi/10605HW5/data/PAndI.txt"));

		job.waitForCompletion(true);
	}
}
