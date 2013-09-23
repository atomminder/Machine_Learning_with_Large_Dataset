/**
 *	10605 BigML
 *	Assignment8: Multi-Class Image Classification
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi
 *  
 *  This class stores global parameters
 *
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;



public class ImageClassifierParallelUB {


	// Store the parameters(vector beta) of the logistic regression 
	private static HashMap<Integer, HashMap<Integer,Double>> weights = new HashMap<Integer, HashMap<Integer,Double>>();
	// Store the vector A in our algorithm
	private static HashMap<Integer, HashMap<Integer,Long>> count = new  HashMap<Integer, HashMap<Integer,Long>> ();
	// Store the probabilities for 100 labels
	private static double[] p = new double[100];

	// initialize the defined hashtable
	public static void initialize() {

		for(int i=1; i <= globalParameters.totalLabel; ++i) {

			weights.put(i,new HashMap<Integer,Double>());
			count.put(i,new HashMap<Integer,Long>());	
		}
	}

	// reconstruct training data for each specific classifier
	static void reconTrainFile(String oldFile, String outFile, int labelIndex) {

		String line = null;
		try{
			// set up read-in file and its buffer
			File infile = new File(oldFile);
			FileInputStream fstream = new FileInputStream(infile);
			BufferedReader instream = new BufferedReader(new InputStreamReader(fstream));
			// set up output file
			File newFile = new File(outFile);
			FileWriter write = new FileWriter(newFile,false);
			BufferedWriter bufferedWriter = new BufferedWriter(write); 

			int count = 0;

			while ((line = instream.readLine()) != null && line.length() !=0){

				++count;
				// if the training example is positive in current setting, output 99 times
				if((count <= labelIndex * 200) && (count > (labelIndex - 1) * 200)) {

					for(int j=0; j<99; ++j) {
						bufferedWriter.write(line);
						bufferedWriter.newLine();
					}

				}
				else {
					bufferedWriter.write(line);
					bufferedWriter.newLine();
				}
			}
			instream.close();
			fstream.close();	
			bufferedWriter.flush();
			write.close();
			bufferedWriter.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}

	}


	// calculate the probabilities of the 14 labels
	private static void calculateP(HashMap<Integer, Double> attributes, int label) {

		p[label-1]  = 0;

		Iterator<Integer> itr = attributes.keySet().iterator();
		while(itr.hasNext()) {

			int attr = itr.next();
			if(weights.get(label).containsKey(attr)) {

				p[label-1] += weights.get(label).get(attr) * attributes.get(attr);
				//p[label-1] += weights.get(label).get(attr);
			}
		}	
		// sigmoid 
		p[label-1]  = sigmoid(p[label-1]);

	}

	// sigmoid function
	static double overflow=20;
	public static double sigmoid(double score) {
		if (score > overflow) score =overflow;
		else if (score < -overflow) score = -overflow;
		double exp = Math.exp(score);
		return exp / (1 + exp);
	}

	public static HashMap<Integer, Double> normalize(String[] attrStr) {

		HashMap<Integer, Double> attrMap = new HashMap<Integer,Double>();

		for(String attr: attrStr) {

			if(attr.length() == 0)
				continue;
			int temp = Integer.parseInt(attr); 
			if(! attrMap.containsKey(temp)) {
				attrMap.put(temp, 1.0);
			}
			else {
				double value = attrMap.get(temp);
				++value;
				attrMap.put(temp, value);
			}
		}
		double L2NormDistance = 0;
		Iterator<Integer> itr = attrMap.keySet().iterator();
		while(itr.hasNext()) { 
			//int key = itr.next();
			L2NormDistance += Math.pow(attrMap.get(itr.next()),2);
		}
		L2NormDistance = Math.sqrt(L2NormDistance);
		itr = attrMap.keySet().iterator();
		while(itr.hasNext()) { 
			int key = itr.next();
			double value = attrMap.get(key);
			value = value / L2NormDistance;
			attrMap.put(key, value);
		}	
		return attrMap;
	}

	// step 2 in our algorithm
	public static void sgd(String line, int t, long k, int labelIndex) {

		// calculate lambda
		double lambda = globalParameters.eta / (t * t);
		String[] elements = line.split("\t");
		// get the label and y
		int y = 0;
		int label = Integer.parseInt(elements[0]);
		if(label == labelIndex) {
			y = 1;
		}



		// get the atrributes
		//String[] attributes = elements[1].split(" ");
		HashMap<Integer, Double> attributes = normalize(elements[1].split(" "));

		// calculate p
		calculateP(attributes, labelIndex);

		// iterate 
		Iterator<Integer> iter = attributes.keySet().iterator();
		while(iter.hasNext()) {

			int attrCrt = iter.next();
			if( ! weights.get(labelIndex).containsKey(attrCrt)) {
				weights.get(labelIndex).put(attrCrt, 0.0);
			}
			if( ! count.get(labelIndex).containsKey(attrCrt)) {
				count.get(labelIndex).put(attrCrt, 0L);
			}

			//Simulate the “regularization” updates
			double temp = weights.get(labelIndex).get(attrCrt);
			temp = temp * Math.pow((1 - 2 * lambda * globalParameters.mu), k - count.get(labelIndex).get(attrCrt));

			temp = temp + lambda * (y - p[labelIndex-1]) * attributes.get(attrCrt);
			//temp = temp + lambda * (y - p[labelIndex-1]);

			weights.get(labelIndex).put(attrCrt, temp);
			count.get(labelIndex).put(attrCrt, k);

		}



	}


	// step 3 in our algorithm, similar to step 2
	public static void sgdFinal(int t, long k, int labelIndex) {

		double lambda = globalParameters.eta / (t * t);

		Iterator<Integer> iter = weights.get(labelIndex).keySet().iterator();
		while(iter.hasNext()) {

			int key2 = iter.next();
			double temp = weights.get(labelIndex).get(key2);
			temp = temp * Math.pow((1 - 2 *lambda * globalParameters.mu), k - count.get(labelIndex).get(key2));
			weights.get(labelIndex).put(key2, temp);
		}
	}

	public static void testPrint() {

		Iterator<Integer> iter1 = weights.keySet().iterator();
		StringBuffer output = new StringBuffer(); 
		// control the format of decimal
		DecimalFormat decfmt = new DecimalFormat("##0.0000");

		while(iter1.hasNext()) {

			output.setLength(0);
			Integer key1 = iter1.next();
			System.out.println("----------------------------------------------");
			System.out.println("label:"+ key1);

			Iterator<Integer> iter2 = weights.get(key1).keySet().iterator();
			while(iter2.hasNext()) {

				Integer key2 = iter2.next();
				double temp = weights.get(key1).get(key2);
				output.append(key2).append(":").append(decfmt.format(temp)).append("\t");
			}
			System.out.println(output);
		}

	}

	static class SimpleThread extends Thread {

		private CountDownLatch latch;
		private int outloop;
		private int inloop;
		private String file;

		public SimpleThread(CountDownLatch latch, int outloop, int inloop,String file){
			this.latch = latch;
			this.outloop = outloop;
			this.inloop = inloop;
			this.file = file;
		}

		@Override
		public void run() {
			//System.out.println(this + " RUNNING."+((outloop-1)*4 + inloop));
			
			// use this variable to index which one-vs-rest classification we are training now.
			int labelIndex = (outloop-1)*4 + inloop;

			String line = null;
			System.out.println("Current training label:"+labelIndex);
			String newTrainFile = file;

			
			//reconTrainFile(file, newTrainFile, labelIndex);

			// the command of shuffing the file
			String cmd = "cat -n "+newTrainFile+ " | sort -R | cut -f2-";
			// iteration count
			int t = 0;
			long k = 0;
			// change here to test!!!!!!!!!!!!
			//while(t < 1) {
			while(t < 5) {

				++t;
				// each use the cmd to shuffle the lines in file
				try {
					Runtime run = Runtime.getRuntime();
					Process pr = run.exec(new String[]{ "bash","-c", cmd });
					BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
					// read in line 

					while ((line=buf.readLine())!=null) {

						++k;
						//System.out.println(line);
						// do SGD
						//System.out.println("I am here");
						sgd(line,t,k,labelIndex);

					}
					//System.out.println("I am here");
					pr.waitFor();
					pr.getInputStream().close();
				} catch (Exception e){
					e.printStackTrace();
				}	

			}
			// do step 3
			sgdFinal(t, k, labelIndex);





			latch.countDown();
		}

	}

	// Main
	public static void main(String[] args) throws IOException, InterruptedException {

		if(args.length < 2) {
			
			System.out.println("Need more arguments.");
			System.out.println("Usage: ImageClassifierParallelUB -trainFile -testFile");
			System.exit(1);
		}
		
		// Initialize hashtable
		initialize();

		// raw training data set
		String trainDataFile  = args[0];
		String line = null;

		// total test documents
		long totalTest = 0;	
		// accurately predicted test documents
		long accTest = 0;

		// Training each one-vs-rest classification
        int outloop = 1;
        
        // change here to test!!!!!
        while(outloop <= 25) {
        //while(outloop <= 1) {
        	
        	//System.out.println("outloop:"+outloop);
            CountDownLatch latch = new CountDownLatch(4);
            for(int i=1; i<5; i++) {
                new SimpleThread(latch,outloop,i,trainDataFile).start();
            }
           
            ++outloop;
            //waiting all child thread
            latch.await();	
        }
		
		// change here to test!!!!!!!!!!!!
		testPrint();

		// Read validation file, predict  
		try{

			// image_val.txt
			File validationFile = new File(args[1]);
			FileInputStream fstream = new FileInputStream(validationFile);
			BufferedReader instream = new BufferedReader(new InputStreamReader(fstream));

			double maxP = 0.0;
			int maxLabel = 0;
			StringBuffer testOutput = new StringBuffer();
			DecimalFormat decfmt = new DecimalFormat("##0.0000");

			// read in a line 
			while ((line = instream.readLine()) != null && line.length() !=0){	

				String[] elements = line.split("\t");
				// get the label and y
				int label = Integer.parseInt(elements[0]);

				// get the atrributes
				HashMap<Integer, Double> attributes = normalize(elements[1].split(" "));

				++totalTest;

				maxP = 0.0;
				maxLabel = 0;
				testOutput.setLength(0);
				testOutput.append(totalTest).append(" ");

				// change here to test!!!!!!!!!!!!
				//for(int i = 1; i <= 1; ++i) {
				for(int i = 1; i <= 100; ++i) {

					calculateP(attributes,i);
					if(p[i-1] > maxP) {

						maxP = p[i-1];
						maxLabel = i;
					}

					// System.out.println(p[i-1]);
					testOutput.append(decfmt.format(p[i-1])).append("\t");
				}
				testOutput.append("trueLabel:").append(label).append("\t");
				testOutput.append("predictLabel:").append(maxLabel).append("\t");
				System.out.println(testOutput.toString());
				if(maxLabel == label) {
					++accTest;
				}


			}
			instream.close();
			fstream.close();
			System.out.printf("Percent correct:%d//%d=%.1f%%\n",accTest,totalTest,((float)accTest/totalTest)*100);
		} 
		catch (Exception e){
			e.printStackTrace();
		}	
	}
}