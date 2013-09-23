import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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
	List<String> canopies = new LinkedList<String>();
	double T1 = 25;
	static HashMap<String, String> centerCanopy = new HashMap<String,String>();


	
	public static boolean sameCanopy(String c,String[] exampleCanopy) {
		
		
		String[] centerCny = centerCanopy.get(c).split(",");
		
		for(String center: centerCny) {
			
			for(String example: exampleCanopy) {
				
				if(center.compareTo(example) == 0) {
					return true;
				}
				
			}
		}
		return false;
		
	}
	

	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroids.path"));
		FileSystem fs = FileSystem.get(conf);
		
		// when initialize, there is no center file, use the canopy file
		if(!fs.exists(centroids))
			centroids = new Path(conf.get("canopy.path"));
		// read in the centroids file
		FSDataInputStream in;
		BufferedReader bufread;
		String strLine;
		in = fs.open(centroids);
		// choose firsr 12 canopy as K centroid
		int number = 0;
		bufread = new BufferedReader(new InputStreamReader(in));
		while ((strLine = bufread.readLine()) != null) {
			centers.add(strLine);
			++ number;
			if(number == 12) {
				break;
			}
		}
		in.close();

		Path canopy = new Path(conf.get("canopy.path"));
		FileSystem fs2 = FileSystem.get(conf);

		// read in the centroids file
		FSDataInputStream in2;
		BufferedReader bufread2;
		String strLine2;
		in2 = fs2.open(canopy);
		bufread2 = new BufferedReader(new InputStreamReader(in2));
		while ((strLine2 = bufread2.readLine()) != null) {
			canopies.add(strLine2);
		}
		in2.close();

		// calculate canopies of centers
		StringBuffer tmp = new StringBuffer();
		for(String center: centers) {

			tmp.setLength(0);
			//tmp.append("\t");
			for(String can: canopies) {

				if(SimpleSimilarity.similaritySP2(center, can) < T1) {

					String id = can.substring(0, can.indexOf(","));
					tmp.append(",").append(id);
				}

			}
			centerCanopy.put(center, tmp.toString());
		}



	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String nearest = null;
		double nearestDistance = Double.MAX_VALUE;

		// ignore the first line in the data set file(just some explanations for the data)
		if(value.toString().charAt(0) == 'c') 
			return;


		String val = value.toString();
		String[] tmp = val.split("\t");
		if(tmp.length < 3)
			return;
		String example = tmp[1];
		String[] canopyCrt = tmp[2].split(",");





		// iteration for each point
		for (String c : centers) {


			if(sameCanopy(c, canopyCrt)) {


				// find the nearest node
				double dist = Distance.measure(c, example.toString());
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
		}

		//find the id of the centroid
		long id = Long.parseLong(nearest.substring(0,nearest.indexOf(",")));
		// output
		context.write(new LongWritable(id), new Text(example));
	}

}
