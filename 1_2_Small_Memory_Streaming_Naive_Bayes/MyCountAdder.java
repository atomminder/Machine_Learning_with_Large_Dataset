/*
	10605 BigML
	Assignment2: streaming Naive Bayes 2 (constrain the size of the memory)
	Name: Qiangjian(Jason) Xi
	Andrew id: qxi
*/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.Hashtable;


public class MyCountAdder {

	/* Store some counts */
	private static Hashtable<String,Integer> CountsHash = new Hashtable<String,Integer>();  
	
	/* Ouput to the stdout */
	private static void PrintCounts(){

		Enumeration<String> em = CountsHash.keys();
		while(em.hasMoreElements()){
			String key = (String)em.nextElement();
			int cnt = (Integer)CountsHash.get(key);
			System.out.println(key + "\t\t" + cnt);	
		}
	}
	
	/* main */
	public static void main(String[] args) throws IOException {

		/* stdin */
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String s;
		
		/* store tokens */
		String[] toks = null;
		
		/* some temps */
		String Vtemp1 = "~";
		String Vtemp2 = null;
		String Ltemp1 = "~";
		String Ltemp2 = null;
		/* Count the vocabulary number */
		int Vcounter = 0;
		/* Count the label number */
		int Lcounter = 0;
		
		/* stdin */
		while ((s = in.readLine()) != null && s.length() != 0){
			
		    toks = s.split("\t\t");
		    
		    //1 find the total documents
		    if(toks[0].compareTo("totalDoc") == 0) {

		    	System.out.println("0totalDoc" + "\t\t" + Integer.parseInt(toks[1]));
		    	continue;
		    }
		    
		    //5 Vocabulary number  
		    else if(toks[0].contains("0V")) {
		    	
		    	// split the key
		    	String[] Vtemp = toks[0].split("\t");	    	
		    	
		    	// find the current word
		    	Vtemp2 = Vtemp[1];
		    	
		    	// if current word do not appear before
		    	if( Vtemp2.compareTo(Vtemp1) != 0 ){
		    		++ Vcounter;
		    		Vtemp1 = Vtemp2;
		    	}
		    	else {
		    		continue;
		    	}
		    }
		    
		    //6 label numbers 
		    else if(toks[0].contains("0L")) {
		    	
		    	// split the key
		    	String[] Ltemp = toks[0].split("\t");
		    	
		    	// find the current word
		    	Ltemp2 = Ltemp[1];
		    	
		    	// if current word do not appear before
		    	if( Ltemp2.compareTo(Ltemp1) != 0 ){
		    		++ Lcounter;
		    		Ltemp1 = Ltemp2;
		    	}
		    	else {
		    		continue;
		    	}
		    }
		    
		    /* others */
		    else {

		    	if(CountsHash.containsKey(toks[0])){
		    		CountsHash.put(toks[0], CountsHash.get(toks[0]) + Integer.parseInt(toks[1]));
		    	}else{
		    		CountsHash.put(toks[0],Integer.parseInt(toks[1]));	
		    	}
		    }
		    
			//If hashtable is larger than a certain size, clear*/
		if(CountsHash.size() > 100000) {

				PrintCounts();
				CountsHash.clear();

			}
		}
		
		/* Output to stdout */
		PrintCounts();
	    System.out.println("0V" + "\t\t"+Vcounter);
	    System.out.println("0L" + "\t\t"+Lcounter);		
	}
}
