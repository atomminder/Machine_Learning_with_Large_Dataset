
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;


/*
 * Simple streaming naive bayes training
 * Reading from stdin, output to stout
 */
public class NBTrain {

	
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
	
	/**main function
	 * 
	 * @param args
	 */
	public static void main(String[] args) {		

		// temp store line in file 
		String line ;
		
		//store total number of training instances
		int totallabel = 0;	
		// for each label y the number of training instances of that class
		HashMap <String, Integer> labelCount = new HashMap <String, Integer>();
		// total number of tokens for documents with label y.
        HashMap<String, Integer> labelWordTotal= new HashMap<String, Integer>();
		// number of times token w appears in a document with label y
		HashMap <String , Integer> wordInLabel = new HashMap <String, Integer>();

		BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(System.in));
		
		// read in each line in file
		try {
			
			while ((line = reader.readLine()) != null){
				
				// split by \t
				String[] splits = line.split("\t");
				// store the labels
				String[] labels = splits[0].split(","); 
				// find the words 
				Vector<String> tokens = tokenizeDoc(splits[1]);
				
				// Iterate each label
				for (String label : labels){
					
					// if the label is end with "CAT"
					if (label.endsWith("CAT")){

						// increase the counter for ending in CAT
						totallabel++;
						
						// if this label is already in the labelCount table, just add 1
						if (labelCount.containsKey(label)){
							labelCount.put(label, labelCount.get(label)+1);
						}
						// If not, put current label in the table
						else{
							labelCount.put(label, 1);
						}
						
						// if this label is already in the labelWordTotal table,
						// just in crease the  totals number of tokens of this label 
                        if (labelWordTotal.containsKey(label)) {
                        	labelWordTotal.put(label, labelWordTotal.get(label) + tokens.size());
						}
                        // if not, put count of tokens in the table
                        else{
                        	labelWordTotal.put(label, tokens.size());
						}
                        
                        // deal with each token in the current training example
    					for (String token : tokens){
    						
    						// store the word counter in each label
    						String key = label + ":" + token;
 
    						if (wordInLabel.containsKey(key)){
    							wordInLabel.put(key, wordInLabel.get(key)+1);
    						}
    						else{
    							wordInLabel.put(key,1);
    						}
    					}     
					}	
				}
			}
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 		
		
		// number of different label  & output total number of training instances
        System.out.println(labelCount.size() + ":" + totallabel ); 
        // output the count for each label
        Iterator <Entry <String ,Integer>> itr1 = labelCount.entrySet().iterator();
        while(itr1.hasNext()){
            Entry <String, Integer> entry  = itr1.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        // output the number of tokens in each label
        Iterator<Entry<String, Integer>> itr2 = labelWordTotal.entrySet().iterator();
        while (itr2.hasNext()) {
            Entry<String, Integer> entry = itr2.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        // output the number of each token in each label
        Iterator<Entry<String, Integer>> itr3 = wordInLabel.entrySet().iterator();
        while (itr3.hasNext()) {
            Entry<String, Integer> entry = itr3.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
	}
}
