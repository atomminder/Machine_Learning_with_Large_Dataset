import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class CanopySelMapper extends 
	Mapper<LongWritable, Text,LongWritable,Text>{
	
	ArrayList<String> canopies = new ArrayList<String> ();
	double T2 = 25;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) {
		
		// ignore the first line in the data set file(just some explanations for the data)
		if(value.toString().charAt(0) == 'c') 
			return;
		
		String val = value.toString();
		if (canopies.size() == 0) {
			canopies.add(val);
			return;
		}
		
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
		
		for(String canopy: canopies) {
			
			context.write(new LongWritable(1), new Text(canopy));
		}
	}

}
