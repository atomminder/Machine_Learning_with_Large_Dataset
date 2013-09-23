

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Vector;
import java.lang.Math;
import java.lang.StringBuffer;


public class NBTest {

	/** 
	 * function - tokenizeDoc
	 * change documents into features
	 * 
	 * @param cur_doc
	 */
    public static Vector<String> tokenizeDoc(String cur_doc) {
        String[] words = cur_doc.split("\\s+");
        Vector<String> tokens = new Vector<String>();
        for (int i = 0; i < words.length; i++) {
            words[i] = words[i].replaceAll("\\W", "");
            if (words[i].length() > 0) {
                tokens.add(words[i]);
            }
        }
        return tokens;
    }

	/** 
	 * function - constructModel
	 * construct the model from std
	 * 
	 * @param wordInLabel
	 * @param labelCount
	 * @param labelWordTotal
	 * @return total number of labels
	 */
	public static int constructModel(HashMap<String, Integer> wordInLabel,
            HashMap<String, Integer> labelCount, HashMap<String, Integer> labelWordTotal) {
		
		// temp store each line
        String line;
        // total number of labels
        int totallabel = 0;
        // different number of labels
        int labelNumber = 0;
        
        BufferedReader myRead = new BufferedReader(new java.io.InputStreamReader(System.in));
        try {
            
            line = myRead.readLine();
            String[] splits = line.split(":");
            // store the different number of labels
            labelNumber = Integer.parseInt(splits[0]);
            // store total number of labels
            totallabel = Integer.parseInt(splits[1]);

            // the count for each label, store in table
            for (int i = 0; i < labelNumber; i++) {
                line = myRead.readLine();
                splits = line.split(":");
                labelCount.put(splits[0], Integer.parseInt(splits[1]));
            }
            // the number of tokens in each label, store in table
            for (int i = 0; i < labelNumber; i++) {
                line = myRead.readLine();
               // System.out.println(line);
                splits = line.split(":");
                labelWordTotal.put(splits[0], Integer.parseInt(splits[1]));
            }
            // the number of each token in each label, store in table
            while ((line = myRead.readLine()) != null) {
            	
                splits = line.split(":");
                wordInLabel.put(splits[0]+":"+splits[1], Integer.parseInt(splits[2]));
            }
        }
        catch (Exception e) {
            System.out.println("Constructing model failed");
        }
        return totallabel;
    }
 
	/**main function
	 * 
	 * @param args
	 */
    public static void main(String[] args) {
        
        // store the total test example
        int testNumber = 0; 
    	// store the number of predicting correctly
        int testTrue = 0; 
		//store total number of training instances
		int totallabel = 0;	
		// for each label y the number of training instances of that class
		HashMap <String, Integer> labelCount = new HashMap <String, Integer>();
		// total number of tokens for documents with label y.
        HashMap<String, Integer> labelWordTotal= new HashMap<String, Integer>();
		// number of times token w appears in a document with label y
		HashMap <String , Integer> wordInLabel = new HashMap <String, Integer>();
        // read from the stdin, then construct the model 
        totallabel = constructModel(wordInLabel, labelCount, labelWordTotal);
        
        BufferedReader myRead;
        FileInputStream infile;
        try {

            infile = new FileInputStream(args[0]);
            myRead = new BufferedReader(new InputStreamReader(infile));
            //temp store each line
            String line;
            // iterate each line(each test example
            while ((line = myRead.readLine()) != null) {
            	

            	// store the maximum log probability 
                double max = -2147483648;
                // store total tokens in the current test example, which is used in smoothing
                int totalWord = 0;
                // store the predicting result
                String predict = "";
                // temp store
                String[] parts = line.split("\t");
                // label of test example
                String[] truelabel = parts[0].split(",");
                
                for (String label : truelabel){
                	if (label.endsWith("CAT")) {
                    	// increase the count
                    	++ testNumber;
                    	break;
                	}
                }
                
                // all the tokens in test example
                Vector<String> tokens = tokenizeDoc(parts[1]);
                
                totalWord = wordInLabel.keySet().size();
                String[] classes = {"CCAT", "ECAT", "GCAT", "MCAT"};
                double[] predicts = {0.0, 0.0, 0.0, 0.0};
                for (int i = 0; i < 4; i++) {
                	
                	// the log probability for current predict label out of all labels
                	predicts[i] = Math.log( (labelCount.get(classes[i])+ 1) / (totallabel * 1.0 + 4 ) );
                	// iterate each token
                    for (String token : tokens) {
                 
                    	/* if this token is in the trainng model for this lable,
                    	 * add related log probability for this token*/ 
                    	if (wordInLabel.get(classes[i] + ":" + token) != null) {

                    		predicts[i] += Math.log((wordInLabel.get(classes[i] + ":" + token) + 1)
                    				/ ((labelWordTotal.get(classes[i]) + totalWord) * 1.0) );
                    	}
                    	/* if this token is not in, just smoothing */
                    	else {

                    		predicts[i] += Math.log( 1.0
                    				/ ((labelWordTotal.get(classes[i]) + totalWord) * 1.0 ) );
                    	}
                    }
                    // find the max result
                    if (predicts[i] > max){
                        max= predicts[i];
                        predict = classes[i];   
                    }
                }
                // construct the output form 
                StringBuffer output1 = new StringBuffer();
                output1.append('[');
                for(int i=0; i < truelabel.length - 1; ++i) {
                	output1.append(truelabel[i]).append(',');
                }
                output1.append(truelabel[truelabel.length - 1]);
                output1.append(']');
                output1.append(" ").append(predict).append('\t').append(max);
                // output
                System.out.println(output1);
                // find whether the predict is the same as true label
                for(int i=0; i < truelabel.length; ++i) {
                    if(truelabel[i].equals(predict)) {
                    	++ testTrue;
                    }
                }    
            }
            // construct the output form 
            StringBuffer output2 = new StringBuffer();
            output2.append("Percent correct: " ).append(testTrue).append('/');
            output2.append(testNumber).append('=');
            output2.append(testTrue * 1.0 / testNumber);
            // output
            System.out.println(output2);
        }
        catch (Exception e) {
            System.out.println("Reading failed");
        }
    }
}