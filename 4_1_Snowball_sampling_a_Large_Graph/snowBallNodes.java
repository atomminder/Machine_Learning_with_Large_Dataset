/**
 *	10605 BigML
 *  Assignment6: Snowball Sampling a Large Graph
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi
 *	
 *	
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;


public class snowBallNodes {

	public static HashMap<String, Double> p = new HashMap<String, Double>();
	public static HashMap<String, Double> r = new HashMap<String, Double>();
	public static HashMap<String, String> nodeAndEdges = new HashMap<String, String> ();
	public static HashSet<String> S = new HashSet<String>();
	public static HashSet<String> SStar = new HashSet<String>();
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

					if(! r.containsKey(words[0])) {

						r.put(words[0], 0.0);
						continue;

					}
					double currentR = r.get(words[0]);
					if(Double.compare(currentR * 1.0 / currentDegree,epsilon) > 0) {

						push(p,r,words);
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


		Iterator<String> iteratorP = p.keySet().iterator();
		while(iteratorP.hasNext()) {

			String key = iteratorP.next();

			if(Double.compare(p.get(key), 0.0) <= 0) {
				p.remove(key);
			}

		}

		//take the nodes that have non-zero weight in p, and include all
		//the edges that are incident on these nodes.
		try {

			File file = new File(args[1]);
			FileInputStream fstream = new FileInputStream(file);
			BufferedReader instream = new BufferedReader(new InputStreamReader(fstream));


			// read in a line 
			while ((line = instream.readLine()) != null && line.length() !=0){

				words = line.split("\t");
				if(p.containsKey(words[0])  && Double.compare(p.get(words[0]), 0.0) > 0) {
					nodeAndEdges.put(words[0], line);
				}
			}

			instream.close();
			fstream.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}

		long volumeS = 0;
		HashSet<String> boundaryS = new HashSet<String>();
		HashSet<String> boundarySStar = new HashSet<String>();
		Double phiS = 0.0;
		Double phiSStar =0.0;
		
		String[] neighborsCrt = null;
		String maxP = null;
		double max = -1.0;

		HashMap<String, Double> pCopy = new HashMap<String, Double>();
		iteratorP = p.keySet().iterator();
		String key = null;
		while(iteratorP.hasNext()) {

			key = iteratorP.next();
			pCopy.put(key, p.get(key));

		}

		// Building a low-conductance subgraph

		// add seed node to S and SStar
		S.add(seed);
		SStar.add(seed);
		// find neighbors
		neighborsCrt = nodeAndEdges.get(seed).split("\t");
		// remove seed node from P
		p.remove(seed);
		// increase the volume
		volumeS += (neighborsCrt.length - 1);
		// add the boundary
		for(int i=1; i<neighborsCrt.length; ++i) {

			if(! S.contains(neighborsCrt[i])) {
				boundaryS.add(seed+':'+neighborsCrt[i]);
				boundarySStar.add(seed+':'+neighborsCrt[i]);
			}
		}
		// Calculate phi
		phiS = boundaryS.size() * 1.0 / volumeS;
		phiSStar = phiS;

		// iterate each p
		while( ! p.isEmpty()) {

			// initialize the value of max

			max = -1.0;	
			// find max element in p
			iteratorP = p.keySet().iterator();
			while(iteratorP.hasNext()) {

				key = iteratorP.next();

				if(Double.compare(p.get(key), max) > 0) {
					max = p.get(key);
					maxP = key;
				}

			}
			
			// add this node 
			S.add(maxP);
			// remove it from the p
			p.remove(maxP);
			// find neighbors
			neighborsCrt = nodeAndEdges.get(maxP).split("\t");
			// increase the volume
			volumeS += (neighborsCrt.length - 1);

			// removing the set of edges that when including current node
			Iterator<String> iteratorSbdr = boundaryS.iterator();
			while(iteratorSbdr.hasNext()) {
				key = iteratorSbdr.next();
				if(key.contains(maxP)) {
					// attention here
					iteratorSbdr.remove();
				}
			}
			// add the boundary
			for(int i=1; i<neighborsCrt.length; ++i) {

				if(! S.contains(neighborsCrt[i])) {
					boundaryS.add(maxP+':'+neighborsCrt[i]);
				}
			}
			// Calculate phi
			phiS = boundaryS.size() * 1.0 / volumeS;

			// compare
			if(phiS < phiSStar) {


				phiSStar = phiS;
				Iterator<String> iteratorS = S.iterator();
				while(iteratorS.hasNext()) {
					key = iteratorS.next();
					if( ! SStar.contains(key))
						SStar.add(key);
				}

			}

		}

		Iterator<String> iteratorSStar = SStar.iterator();
		while(iteratorSStar.hasNext()) {
			key = iteratorSStar.next();
			System.out.println(key+'\t'+ pCopy.get(key));
		}
	}
}
