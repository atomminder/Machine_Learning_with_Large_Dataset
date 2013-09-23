/*
 * Deal with the doc 
 * andrew id: qxi
 * name: Qiangjian Xi 
 * 
 */
import java.util.Vector;

public class NBHelper {


	protected static Vector<String> tokenizeDoc(String cur_doc) {
		
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
	
	protected static String[] getLabels(String doc) {
		if(doc == null)
			return null;
		return doc.substring(0, doc.indexOf('\t')).trim().split(",");

	}
	
}
