/*
	10605 BigML
	Assignment2: streaming Naive Bayes 2 (constrain the size of the memory)
	Name: Qiangjian(Jason) Xi
	Andrew id: qxi
*/

import java.io.*;
import java.util.*;

public class NBTest{

	/* Total documents */
	private static int totalDocs = 0; 
	/* Total vocabulary */
	private static int totalVocs = 0; 
	/* Total labels*/
	private static int totalLabs = 0; 
	/* store some counts */
	private static Hashtable<String,Integer> CountsHash = new Hashtable<String,Integer>();
	/* store the word counts of each label */
	private static Hashtable<String,Integer> labelWordCount = new Hashtable<String,Integer>(); 
	/* store the label counts */
	private static Hashtable<String,Integer> labelCount = new Hashtable<String,Integer>(); 	
	

	/* Tokenize the document */
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
	
	
	/* main */
	public static void main(String[] args) throws IOException {

		String s;
		// total test documents
	    int totalTestDocs = 0;	
	    // accurately predicted test documents
	    int accTestDocs = 0;			
	  
		// Read test file, find all the corpus
		if(args[0].matches("-t")){

			try{
				File file = new File(args[1]);
				FileInputStream fstream = new FileInputStream(file);
				BufferedReader instream = new BufferedReader(new InputStreamReader(fstream));
				while ((s = instream.readLine()) != null && s.length() !=0){	
					
					String[] words = s.split("\\s+");
	    			Vector<String> sTokens = tokenizeDoc(words);	    		    

	    		    for(int i=0;i<sTokens.size();i++)
	    		    {						
	    		    	String yw_key = sTokens.get(i);
	    		    	/* if current word not in the table, add in */
	    		    	if( !CountsHash.containsKey(yw_key)){	
	    		    		CountsHash.put(yw_key,1);      					
	    		    	}
	    		    }
				}
				instream.close();
	    		fstream.close();	    	
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
		else{

			System.out.println("Usage: cat <train-file> | java NBTrain | sort -k1,1 | java NBTest -t <test-file>");
		}
		
		// read in the training model 
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String[] toks = null;
		while ((s = in.readLine()) != null && s.length() != 0){

			/* total document */
			toks = s.split("\t\t");
			if(toks[0].contains("0totalDoc") ){

				totalDocs = Integer.parseInt(toks[1]);
			}
			/* total labels */
			else if(toks[0].contains("0L") ){

				totalLabs = Integer.parseInt(toks[1]);
			}
			/* total vocabulary */
			else if(toks[0].contains("0V") ){

				totalVocs = Integer.parseInt(toks[1]);
			}
			/* words number in each label */
			else if(toks[0].contains("0W") ){

				String[] tmp = toks[0].split("\t");
				labelWordCount.put(tmp[1], Integer.parseInt(toks[1]));

			}
			/* label count */
			else if(toks[0].startsWith("0")) {

				labelCount.put(toks[0].substring(1), Integer.parseInt(toks[1]));
			}
			/* input is label\tword\t\tcounter, find whether this word in is in the text, 
			 * if in, ass in the countsHash */
			else {
				
				String[] tmp = toks[0].split("\t");
				if(CountsHash.containsKey(tmp[1])) {
					CountsHash.put(toks[0], Integer.parseInt(toks[1]));
				}
			}
		}


		// Read test file, predict  
		try{
			
			File file = new File(args[1]);
			FileInputStream fstream = new FileInputStream(file);
			BufferedReader instream = new BufferedReader(new InputStreamReader(fstream));
			
			/* read in a line */
			while ((s = instream.readLine()) != null && s.length() !=0){	

				String[] words = s.split("\\s+");
    			String l = words[0]; 
    			Vector<String> sTokens = tokenizeDoc(words);
    			/*store the predicted log probabilities */
    			Hashtable<String,Double> lnprobs = new Hashtable<String,Double>(); 
    			Enumeration<String> em1 = labelCount.keys();
    			while(em1.hasMoreElements()){
    				String label = (String)em1.nextElement();
    				lnprobs.put(label, 0.0);
    			}
    			/* for each label */
    			Enumeration<String> em2 = labelCount.keys();
    			while(em2.hasMoreElements()){
    				
    				String label = (String)em2.nextElement();
    				for(int i=0;i<sTokens.size();i++){
    					
    					int yw_cnt = 0;
    					String yw_key = label + "\t" + sTokens.get(i);
    					if(CountsHash.containsKey(yw_key)){
    						yw_cnt = CountsHash.get(yw_key);	
    					}
    					lnprobs.put(label, lnprobs.get(label) + Math.log(yw_cnt + 1) - Math.log(labelWordCount.get(label) + totalVocs));
    					
    				}
    				lnprobs.put(label, lnprobs.get(label) + Math.log(labelCount.get(label) + 1) - Math.log(totalDocs + totalLabs));
    				
    			}
    			
    			
    			/* find the max one */
    			Double maxValue = -1000000.0;
    			String pred_label = null;
    			Enumeration<String> em3= lnprobs.keys();
    			while(em3.hasMoreElements()){
    				String label = (String)em3.nextElement();
    				if(lnprobs.get(label) > maxValue) {
    					maxValue = lnprobs.get(label);
    					pred_label = label;
    				}
    			}
    			System.out.println("[" +l+ "]\t" +pred_label+ "\t" +maxValue);

    			String[] labels = l.split(",");
    			/* whether it matches the original labels */
    			for(String label : labels){     	
    				if(label.matches(pred_label)){
    					accTestDocs++;
    				}
    			}
    			totalTestDocs++;	
							
			}
			instream.close();
    		fstream.close();
    		System.out.printf("Percent correct: %d/%d=%.1f%%\n",accTestDocs,totalTestDocs,((float)accTestDocs/totalTestDocs)*100);

		} 
		catch (Exception e){
			e.printStackTrace();
		}
	}	
}
