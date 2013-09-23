package edu.cmu;
/**
 *	10605 BigML
 *  Assignment7: K-Means Clustering with canopy selection on MapReduce
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi
 *	
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AssignCanopy extends  
	Mapper<LongWritable, Text,LongWritable,Text>{

	// canopy set
	ArrayList<String> canopies = new ArrayList<String>();
	// threshold
	double T1 = 25;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		
		// Read in the canopy set 
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("canopy.path"));
		FileSystem fs = centroids.getFileSystem(conf);
		FSDataInputStream in;
		BufferedReader bufread;
		String strLine;
		in = fs.open(centroids);
		bufread = new BufferedReader(new InputStreamReader(in));
		while ((strLine = bufread.readLine()) != null) {
			canopies.add(strLine);
		}
		in.close();

	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String val = value.toString();
		StringBuffer canopyStr = new StringBuffer();
		canopyStr.append("\t");
		
		// ignore the first line in the data set file(just some explanations for the data)
		if(value.toString().charAt(0) == 'c') 
			return;
		for(String canopy: canopies) {
			
			// Calculate the similarity, then decide whether take it as its canopy
			// if it is, add in
			if(SimpleSimilarity.similaritySP2(canopy, val) < T1) {
				
				String id = canopy.substring(0, canopy.indexOf(","));
				canopyStr.append(",").append(id);
				
			}
		}
		// write out the key-value pairs
		context.write(new LongWritable(1), new Text(val+canopyStr.toString()));	
	}
}
