

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.text.DecimalFormat;

// calculate a new clustercenter for these vertices
public class KMeansReducer extends
Reducer<LongWritable, Text, LongWritable, Text> {

	public static enum Counter {
		CONVERGED
	}

	List<String> centers = new LinkedList<String>();
	List<String> newCenters = new LinkedList<String>();

	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroids.path"));
		FileSystem fs = FileSystem.get(conf);

		if(fs.exists(centroids)){

			FSDataInputStream in;
			BufferedReader bufread;
			String strLine;
			in = fs.open(centroids);
			bufread = new BufferedReader(new InputStreamReader(in));
			while ((strLine = bufread.readLine()) != null) {
				centers.add(strLine);
			}

			in.close();
		}

	}




	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {


		String[] attrsStr = null;
		double[] attrs = new double[69];
		Double count = 0.0;
		boolean flag = true;
		for(Text val: values) {

			attrsStr = val.toString().split(",|:");
			count += Double.parseDouble(attrsStr[attrsStr.length - 1]);
			for(int i=0; i<attrs.length; ++i){
				attrs[i] += Double.parseDouble(attrsStr[i]);
			}

		}
		for(int i=0; i<attrs.length; ++i){
			attrs[i] = Double.parseDouble(attrsStr[i]) / count;
		}
		StringBuffer  out= new StringBuffer() ;
		// append the index
		out.append(key).append(",");

		
		DecimalFormat deformat = new DecimalFormat("##0.00000");
		
		for(int i=1; i<attrs.length - 1; ++i){
			out.append(deformat.format(attrs[i])).append(",");
		}
		out.append(deformat.format(attrs[attrs.length - 1]));

		// store in newCenters
		newCenters.add(out.toString());
		// find whether it is an old centroid
		for(String center: centers) {
			if (center.compareTo(out.toString()) == 0) {
				flag = false;
				break;
			}
		}
		// if it is old, no increment
		if (flag)
			context.getCounter(Counter.CONVERGED).increment(1);
		// output
		context.write(key, new Text(out.toString()));

	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		super.cleanup(context);


		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroids.path"));
		FileSystem fs = FileSystem.get(conf);
		// delete the old centroid file
		fs.delete(centroids, true);
		// new centroid file
		FSDataOutputStream out = fs.create(centroids);
		for(String center: newCenters) {
			
			
			
			out.writeBytes(center);
			out.writeByte('\n');
		}

		out.close();

	}
}
