/**
 *	10605 BigML
 *  Assignment6: Snowball Sampling a Large Graph
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi	
 *	
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;



public class PangRankApprox {

	public static HashMap<String, Double> p = new HashMap<String, Double>();
	public static HashMap<String, Double> r = new HashMap<String, Double>();
	public static Double epsilon = 0.00001;
	public static Double alpha = 0.3;
	public static String seed = null;

	public static void push(HashMap<String, Double> p, HashMap<String, Double> r, String[] neighbors) {

		String u = neighbors[0];

		double currentRU = r.get(u);

		String v = null;
		double temp = 0.0;
		long degree = (long) (neighbors.length - 1);

		if(! p.containsKey(u)) {
			p.put(u,alpha * currentRU);
		}
		else {
			temp = p.get(u);
			temp += alpha * currentRU;
			p.put(u, temp);
		}

		temp = (1-alpha) * currentRU / 2;
		r.put(u, temp);


		// all the neighbors
		for(int i=1; i<neighbors.length; ++i) {

			v = neighbors[i];

			if(! r.containsKey(v)) {
				r.put(v, (1-alpha) * currentRU / (2 * degree));
			}
			else {
				temp = r.get(v);
				temp += (1-alpha) * currentRU / (2 * degree);
				r.put(v, temp);
			}

		}
	}

	public static void main(String[] args) throws IOException {

		boolean flag = true;

		String line = null;
		String[] words = null;

		// seed string 
		seed = args[0];
		p.put(seed, 0.0);
		r.put(seed, 1.0);

		while(flag) {

			flag = false;

			// Read graph file 
			try {

				File file = new File(args[1]);
				FileInputStream fstream = new FileInputStream(file);
				BufferedReader instream = new BufferedReader(new InputStreamReader(fstream));

				// read in a line 
				while ((line = instream.readLine()) != null && line.length() !=0){

					long currentDegree = 0;

					words = line.split("\t");

					// if the degree of a node is zero, then ignore it.
					if(words.length == 1)
						continue;

					// store the degree of current node u
					currentDegree = (long)(words.length - 1);
					
					// if not in the hash table r, just ignore this line
					if(! r.containsKey(words[0])) {

						r.put(words[0], 0.0);
						continue;

					}
					// get the value of current node
					double currentR = r.get(words[0]);

					// calculate r(u)/d(u), find whether it is bigger than epsilon
					if(Double.compare(currentR * 1.0 / currentDegree,epsilon) > 0) {

						// push 
						push(p,r,words);
						// changes, we need another iteration
						flag = true;
					}
				}
				instream.close();
				fstream.close();
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
		
		// output
		Iterator<String> iteratorP = p.keySet().iterator();
		while(iteratorP.hasNext()) {

			String key = iteratorP.next();
			System.out.println(key+' '+p.get(key)+' ');
		}
		System.out.println();
	}
}
