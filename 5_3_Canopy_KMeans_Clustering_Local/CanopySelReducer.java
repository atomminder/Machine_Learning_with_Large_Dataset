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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CanopySelReducer  extends
Reducer<LongWritable, Text, LongWritable, Text> {

	// final canopy set
	ArrayList<String> canopiesFinal = new ArrayList<String> ();
	// threshold
	double T2 = 25;

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		// iterate each value, doing same thing as the mapper step
		for(Text value: values) {

			boolean flag = true;
			String val = value.toString();
			if (canopiesFinal.size() == 0) {
				canopiesFinal.add(val);
				continue;
				//return;
			}
			// Calculate the similarity, then decide whether take it as a canopy
			for(String finalCanopy: canopiesFinal) {
				if(SimpleSimilarity.similaritySP2(finalCanopy,val) < T2) {
					flag = false;
				}
			}
			if(flag) {
				canopiesFinal.add(val);
			}
		}
	}


	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		super.cleanup(context);

		// Write the canopy set into file
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("canopy.path"));
		FileSystem fs = centroids.getFileSystem(conf);
		fs.delete(centroids, true);
		FSDataOutputStream out = fs.create(centroids);
		for(String center: canopiesFinal) {
			out.writeBytes(center);
			out.writeByte('\n');
		}
		out.close();
	}
}
