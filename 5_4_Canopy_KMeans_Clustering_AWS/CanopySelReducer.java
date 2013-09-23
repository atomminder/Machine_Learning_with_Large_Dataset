import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class CanopySelReducer  extends
Reducer<LongWritable, Text, LongWritable, Text> {

	ArrayList<String> canopiesFinal = new ArrayList<String> ();
	double T2 = 25;

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		for(Text value: values) {

			boolean flag = true;
			String val = value.toString();
			if (canopiesFinal.size() == 0) {
				canopiesFinal.add(val);
				continue;
				//return;
			}

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

		// test
		//System.out.println(canopiesFinal.size());
//		for(String canopy: canopiesFinal) {
//
//			System.out.println(canopy);
//		}

		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("canopy.path"));
		//FileSystem fs = FileSystem.get(URI.create(uri),conf);
		FileSystem fs = centroids.getFileSystem(conf);
		// delete the old centroid file
		fs.delete(centroids, true);
		// new centroid file
		FSDataOutputStream out = fs.create(centroids);
		for(String center: canopiesFinal) {
			out.writeBytes(center);
			out.writeByte('\n');
		}

		out.close();
		
		
	}

}
