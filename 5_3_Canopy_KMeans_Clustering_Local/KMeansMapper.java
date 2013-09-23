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

	// store centroids
	List<String> centers = new LinkedList<String>();
	// store canopies
	List<String> canopies = new LinkedList<String>();
	// threshold
	double T1 = 25;
	// assign each centroid its canopies
	static HashMap<String, String> centerCanopy = new HashMap<String,String>();

	// check whether in the samee canopy
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
	protected void setup(Context context) throws IOException, InterruptedException {
		
		super.setup(context);

		/* read in the centroids file */
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		// when initialize, there is no center file, use the canopy file
		if(!fs.exists(centroids))
			centroids = new Path(conf.get("canopy.path"));
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
		
		/* read in the canopy file */
		Path canopy = new Path(conf.get("canopy.path"));
		FileSystem fs2 = FileSystem.get(conf);
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
			// Calculate the similarity
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
		String example = tmp[1];
		String[] canopyCrt = tmp[2].split(",");

		// iteration for each point
		for (String c : centers) {

			// if center and training example is in the same canopy, continue 
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
