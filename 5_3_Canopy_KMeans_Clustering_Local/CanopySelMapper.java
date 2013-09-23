package edu.cmu;
/**
 *	10605 BigML
 *  Assignment7: K-Means Clustering with canopy selection on MapReduce
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi
 *	
 */
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CanopySelMapper extends 
	Mapper<LongWritable, Text,LongWritable,Text>{
	
	// canopy candidate set
	ArrayList<String> canopies = new ArrayList<String> ();
	// threshold
	double T2 = 25;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) {
		
		// ignore the first line in the data set file(just some explanations for the data)
		if(value.toString().charAt(0) == 'c') 
			return; 
		// There is no canopy in list now, put current example in.
		String val = value.toString();
		if (canopies.size() == 0) {
			canopies.add(val);
			return;
		}
		// Calculate the similarity, then decide whether take it as a canopy candidate
		for(String canopy: canopies) {
			
			if(SimpleSimilarity.similaritySP2(canopy,val) < T2) {
				return;
			}
		}
		canopies.add(val);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		super.cleanup(context);
		
		// write the key-value pairs
		for(String canopy: canopies) {
			
			context.write(new LongWritable(1), new Text(canopy));
		}
	}

}
