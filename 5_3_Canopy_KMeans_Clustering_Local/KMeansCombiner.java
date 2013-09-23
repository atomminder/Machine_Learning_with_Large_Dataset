package edu.cmu;
/**
 *	10605 BigML
 *  Assignment7: K-Means Clustering with canopy selection on MapReduce
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi
 *	
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {


	// pay attention to the name of this function, it should not be combine()
	public void reduce(LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		StringBuffer out = new StringBuffer();
		String[] numStr = null; ;

		double [] numbers = new double[69];
		long count = 0;
		
		// combine
		for (Text val : values) {

			++ count;
			numStr = val.toString().split(",");
			for(int i=0; i< numStr.length; ++i) {
				
				numbers[i] += Long.parseLong(numStr[i]);
			}
		}
		for(int i=0; i< numbers.length - 1; ++i) {
			out.append(numbers[i]+",");
		}
		out.append(numbers[numbers.length - 1]+":");
		out.append(count);
		context.write(key, new Text(out.toString()));
	}
}