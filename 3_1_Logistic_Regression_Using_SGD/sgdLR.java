/**
 *	10605 BigML
 *  Assignment5: Logistic Regression Using Stochastic Gradient Descent
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi
 *	
 *	This is the main class for LR using SGD
 *	
 *	
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;


public class sgdLR {

	// Store the parameters(vector beta) of the logistic regression 
	private static HashMap<String, HashMap<Long,Double>> weights = new HashMap<String, HashMap<Long,Double>>();
	// Store the vector A in our algorithm
	private static HashMap<String, HashMap<Long,Long>> count = new  HashMap<String, HashMap<Long,Long>> ();
	// Store the probabilities for 14 labels
	private static double[] p = new double[14];

	// convert a string to an id between 0 and N 
	private static long wordMap(String word) {

		long id = word.hashCode() % globalParameters.vocalbulary;
		if (id<0) 
			id+=  globalParameters.vocalbulary;
		return id;
	}

	// Tokenize the document 
	private static Vector<String> tokenizeDoc(String[] words){		
		Vector<String> tokens = new Vector<String>();
		for (int i = 1; i < words.length; i++) {
			words[i] = words[i].replaceAll("\\W", "");
			if (words[i].length() > 0) {
				tokens.add(words[i]);
			}
		}
		return tokens;	
	}

	// initialize the defined hashtable
	public static void initialize() {

		for(int i=0; i < globalParameters.AllLabels.length; ++i) {

			weights.put(globalParameters.AllLabels[i],new HashMap<Long,Double>());
			count.put(globalParameters.AllLabels[i],new HashMap<Long,Long>());	
		}
	}

	// calculate the probabilities of the 14 labels
	private static void calculateP(Vector<String> sTokens) {

		// initialize to all zeros
		for(int i=0; i<14; ++i) {
			p[i] = 0.0;
		}
		// calculate probabilities
		for(int i=0;i<sTokens.size();i++)
		{
			// convert the word to number 
			long j = wordMap(sTokens.get(i));
			// calculate for each label
			for(int m=0; m<globalParameters.AllLabels.length; ++m) {

				String labelCrt = globalParameters.AllLabels[m];
				if(weights.get(labelCrt).containsKey(j)) {

					p[globalParameters.findLabelIndex(labelCrt)] += 
							//weights.get(labelCrt).get(j) * sTokens.get(i).hashCode();
							weights.get(labelCrt).get(j);
				}		
			}		
		}
		// sigmoid 
		for(int i=0; i<14; ++i) {
			p[i] = sigmoid(p[i]);
		}
	}


	// step 2 in our algorithm
	public static void sgd(String line, int t, long k) {

		// calculate lambda
		double lambda = globalParameters.eta / (t * t);

		String[] words = line.split("\\s+");
		String labelStr = words[0];
		Vector<String> sTokens = tokenizeDoc(words);
		// current labels of this line
		String[] labels = labelStr.split(",");
		// find the value of y
		int[] y = new int[14];
		for(String exampleLabel: labels) {		
			y[globalParameters.findLabelIndex(exampleLabel)] = 1;			
		}
		// calculate p
		calculateP(sTokens);

		// iterate 
		for(int i=0;i<sTokens.size();i++)
		{

			// convert word to number
			long j = wordMap(sTokens.get(i));
			// for each label, update
			for(int m=0; m<globalParameters.AllLabels.length; ++m) {

				String labelCrt = globalParameters.AllLabels[m];
				if( ! weights.get(labelCrt).containsKey(j)) {
					weights.get(labelCrt).put(j, 0.0);
				}
				if( ! count.get(labelCrt).containsKey(j)) {
					count.get(labelCrt).put(j, 0L);
				}
				//Simulate the “regularization” updates
				double temp = weights.get(labelCrt).get(j);
				temp = temp * Math.pow((1 - 2 * lambda * globalParameters.mu), k - count.get(labelCrt).get(j));

				//temp = temp + lambda * (y[globalParameters.findLabelIndex(labelCrt)] - p[globalParameters.findLabelIndex(labelCrt)]) * sTokens.get(i).hashCode();
				temp = temp + lambda * (y[globalParameters.findLabelIndex(labelCrt)] - p[globalParameters.findLabelIndex(labelCrt)]);

				weights.get(labelCrt).put(j, temp);
				count.get(labelCrt).put(j, k);
			}
		}
	}

	// step 3 in our algorithm, similar to step 2
	public static void sgdFinal(int t, long k) {

		double lambda = globalParameters.eta / (t * t);

		Iterator<String> iter1 = weights.keySet().iterator();
		while(iter1.hasNext()) {
			String key1 = iter1.next();
			Iterator<Long> iter2 = weights.get(key1).keySet().iterator();
			while(iter2.hasNext()) {

				long key2 = iter2.next();
				double temp = weights.get(key1).get(key2);
				temp = temp * Math.pow((1 - 2 *lambda * globalParameters.mu), k - count.get(key1).get(key2));
				weights.get(key1).put(key2, temp);
			}
		}
	}


	// sigmoid function
	static double overflow=20;
	public static double sigmoid(double score) {
		if (score > overflow) score =overflow;
		else if (score < -overflow) score = -overflow;
		double exp = Math.exp(score);
		return exp / (1 + exp);
	}


	// Main
	public static void main(String[] args) throws IOException, InterruptedException {

		// Initialize hashtable
		initialize();

		int t = 0;
		long k = 0;
		String cmd = "cat -n "+args[0]+ " | sort -R | cut -f2-";
		String line = "";

		// total test documents
		int totalTestDocs = 0;	
		// accurately predicted test documents
		int accTestDocs = 0;

		while(t < 20) {

			++t; 
			// each use the cmd to shuffle the lines in file
			Runtime run = Runtime.getRuntime();
			Process pr = run.exec(new String[]{ "bash","-c", cmd });
			BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));

			// read in line 
			while ((line=buf.readLine())!=null) {

				++k;
				// do SGD
				sgd(line,t,k);

			}
			// do step 3
			sgdFinal(t, k );
			pr.waitFor();
			pr.getInputStream().close();
		}

		// Read test file, predict  
		try{

			File file = new File(args[1]);
			FileInputStream fstream = new FileInputStream(file);
			BufferedReader instream = new BufferedReader(new InputStreamReader(fstream));
			// control the format of decimal
			DecimalFormat decfmt = new DecimalFormat("##0.00000");

			// read in a line 
			while ((line = instream.readLine()) != null && line.length() !=0){	

				String[] words = line.split("\\s+");
				String l = words[0]; 
				Vector<String> sTokens = tokenizeDoc(words);
				String labelCrt = null;
				calculateP(sTokens);

				StringBuffer output = new StringBuffer();

				// calculate accuracy 
				for(int i=0; i<14; ++i) {

					labelCrt = globalParameters.AllLabels[i];
					output.append(labelCrt).append(' ').append(decfmt.format(p[i])).append(' ');
					if(p[i] > 0.5) {

						if (l.toString().contains(labelCrt)) {
							++accTestDocs;
						}
					}
					else {

						if(! l.toString().contains(labelCrt)) {
							++accTestDocs;
						}
					}
				}
				System.out.println("[" +l+ "]\t" +output);
				totalTestDocs += 14;
			}
			instream.close();
			fstream.close();
			System.out.printf("Percent correct: %.1f%%\n",((float)accTestDocs/totalTestDocs)*100);
		} 
		catch (Exception e){
			e.printStackTrace();
		}
	}
}
