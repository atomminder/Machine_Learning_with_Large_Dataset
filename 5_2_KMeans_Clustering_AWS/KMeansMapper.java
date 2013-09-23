import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



// calculate centroid for each data point 
public class KMeansMapper extends
		Mapper<LongWritable,Text,LongWritable,Text> {

	List<String> centers = new LinkedList<String>();
	//String uri = "hdfs://10.198.91.16:9000";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		//FileSystem fs = FileSystem.get(URI.create(uri),conf);
		FileSystem fs = centroids.getFileSystem(conf);
		
		// read in the centroids file
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

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String nearest = null;
		double nearestDistance = Double.MAX_VALUE;
		
		// ignore the first line in the data set file(just some explanations for the data)
		if(value.toString().charAt(0) == 'c') 
			return;
		
		// iteration for each point
		for (String c : centers) {

			// find the nearest node
			double dist = Distance.measure(c, value.toString());
			if (nearest == null) {
				nearest = c;
				nearestDistance = dist;
			} 
			else {
				if (nearestDistance > dist) {
					nearest = c;
					nearestDistance = dist;
				}
			}
		}
		
		//find the id of the centroid
		long id = Long.parseLong(nearest.substring(0,nearest.indexOf(",")));
		// output
		context.write(new LongWritable(id), value);
	}

}
