import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/* step 6: compute the phraseness and informativeness scores as described in the paper*/
public class Rank {
	
	
	public static class MyEntry<K, V> implements Map.Entry<K, V> {
	    private final K key;
	    private V value;

	    public MyEntry(K key, V value) {
	        this.key = key;
	        this.value = value;
	    }

	    @Override
	    public K getKey() {
	        return key;
	    }

	    @Override
	    public V getValue() {
	        return value;
	    }

	    @Override
	    public V setValue(V value) {
	        V old = this.value;
	        this.value = value;
	        return old;
	    }
	}
	
	public static class CompareByValue implements Comparator<Map.Entry<String, String>>{

		@Override
		public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
			
			String[] words1 = o1.getValue().split("\t");
			String[] words2 = o2.getValue().split("\t");	
			Double value1 = Double.parseDouble(words1[0]);
			Double value2 = Double.parseDouble(words2[0]);
			if (value2 > value1) {
				return 1;
			}
			else if (value2 == value1) {
				return 0;
			}
			else
				return -1;
		}
	}
	
	public static void main(String args[]) throws IOException {

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String inLine = null;
		String previous= "-";
		StringBuffer output = new StringBuffer();
		String words[] = null;
		StringBuffer tempstr = new StringBuffer();
		Double Phraseness = 0.0;
		Double Informativeness = 0.0;
		Double singleScore = 0.0;

 
		/* Total vocabulary */
		long vocabulary = 0; 
		long phrases = 0;
		long uniBgWords = 0;
		long uniFgWords = 0;
		long biBgPhrases = 0;
		long biFgPhrases = 0;
		
		/* store some counts */
		Hashtable<String,String > scores = new Hashtable<String, String>();
		for(int i=0; i<20; ++i) {
			
			scores.put(Integer.toString(i), "-100000.0");
		}

		/* store the word counts of each label */

		/* First line is uni_global_things */
		inLine = in.readLine();
		words = inLine.split("\t");
		
		String[] temp = words[0].split(" ");
		vocabulary = Long.parseLong(temp[1]);
		temp = words[1].split(" ");
		uniBgWords = Long.parseLong(temp[1]);	
		temp = words[2].split(" ");
		uniFgWords = Long.parseLong(temp[1]);

		/* Second line is bi_global_things */
		inLine = in.readLine();
		words = inLine.split("\t");
		
		temp = words[0].split(" ");
		phrases = Long.parseLong(temp[1]);
		temp = words[1].split(" ");
		biBgPhrases = Long.parseLong(temp[1]);	
		temp = words[2].split(" ");
		biFgPhrases = Long.parseLong(temp[1]);		
		

		
		ArrayList<Map.Entry<String, String>> top20 = new ArrayList<Entry<String, String>>(scores.entrySet());

		
		
		
		
		/* void line */
		inLine = in.readLine();
		/* every line */
		while( (inLine = in.readLine()) != null && inLine.length() != 0 ) {

			words = inLine.split("\t");
			
			
			/* First two columns must be Bx count, Cx count */
			temp = words[1].split(" ");
			long Bx = Long.parseLong(temp[1]);
			
			temp = words[2].split(" ");
			long Cx = Long.parseLong(temp[1]);
			long By = 0;
			long Cy = 0;
			long Bxy = 0;
			long Cxy = 0;
			
			/* If Middle two columns are  Bx count & Cx count , then last two columns are Bxy count & Cxy Count
			 * If Middle two columns are  Bxy count & Cxy Count , then last two columns are By count & Cy count*/
				
			/* Middle two columns may be Bx count, Cx count or Bxy count, Cxy Count */
			temp = words[3].split(" ");
			if(temp[0].compareTo("Bx") == 0) {
				By = Long.parseLong(temp[1]);
				temp = words[4].split(" ");
				Cy = Long.parseLong(temp[1]);
			}
			else {
				 Bxy = Long.parseLong(temp[1]);
				temp = words[4].split(" ");
				 Cxy = Long.parseLong(temp[1]);
			}
			
			/* last two columns may be By count, Cy count or Bxy count, Cxy Count */
			temp = words[5].split(" ");
			if(temp[0].compareTo("By") == 0) {
				 By = Long.parseLong(temp[1]);
				temp = words[6].split(" ");
				 Cy = Long.parseLong(temp[1]);
			}
			else {
				 Bxy = Long.parseLong(temp[1]);
				temp = words[6].split(" ");
				 Cxy = Long.parseLong(temp[1]);
			}
			
			/* Compute probability */
			Double PCxy = 0.0;

				
			PCxy = (Cxy * 1.0 + 1) / (biFgPhrases + phrases);

			Double PCx = 0.0;

			PCx = (Cx * 1.0 + 1) / (uniFgWords + vocabulary);

			
			Double PCy = 0.0;

			PCy = (Cy * 1.0 + 1) / (uniFgWords + vocabulary);

			Double PBxy = 0.0;

			PBxy = (Bxy * 1.0 + 1) / (biBgPhrases + phrases);

			/* Compute the Phraseness*/
			Phraseness = Math.log(PCxy) - Math.log(PCx) - Math.log(PCy);
			Phraseness = Phraseness * (PCxy) ;
			
			/* mutiply tuning parameters*/
			//Phraseness *= 0.1;

			/* Compute the Informativeness*/
			Informativeness = Math.log(PCxy) - Math.log(PBxy);
			Informativeness = Informativeness * (PCxy);
			/* mutiply tuning parameters*/
			//Informativeness *= 1000;
			singleScore = Phraseness + Informativeness;
			tempstr.append(singleScore).append('\t').append(Phraseness).append('\t').append(Informativeness);
			
			
			top20.add(new MyEntry<String, String>(words[0], tempstr.toString()));
			tempstr.setLength(0);
			Collections.sort(top20, new CompareByValue());
			top20.remove(top20.size()-1);
			


		}
		
		
		System.out.println();
		System.out.println();
		for(Entry<String, String> value : top20){
			System.out.print(value.getKey());
			System.out.print("\t");
			System.out.print(value.getValue());
			System.out.println();
		}


		
		
	}

}
