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
import org.apache.hadoop.mapreduce.Mapper.Context;


public class AssignCanopy extends  
	Mapper<LongWritable, Text,LongWritable,Text>{
	
	ArrayList<String> canopies = new ArrayList<String>();
	double T1 = 25;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("canopy.path"));
		//FileSystem fs = FileSystem.get(URI.create(uri),conf);
		FileSystem fs = centroids.getFileSystem(conf);
		
		// read in the canopies file
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
			
			if(SimpleSimilarity.similaritySP2(canopy, val) < T1) {
				
				String id = canopy.substring(0, canopy.indexOf(","));
				canopyStr.append(",").append(id);
				
			}
		}
		
		
		context.write(new LongWritable(1), new Text(val+canopyStr.toString()));
		
		
	}

}
