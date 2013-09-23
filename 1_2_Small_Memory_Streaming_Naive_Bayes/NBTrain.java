/*
	10605 BigML
	Assignment2: streaming Naive Bayes 2 (constrain the size of the memory)
	Name: Qiangjian(Jason) Xi
	Andrew id: qxi
*/

import java.io.*;
import java.util.*;

public class NBTrain{

	/* Total documents */
	private static int totalDocs = 0; 

	/* Store some counts */
	private static Hashtable<String,Integer> CountsHash = new Hashtable<String,Integer>(); 

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
		
		// read data from stdin
		while ((s = in.readLine()) != null && s.length() != 0){
			
			String[] words = s.split("\\s+");
			String l = words[0]; 
			Vector<String> sTokens = tokenizeDoc(words);

			/* current labels of this line*/
			String[] labels = l.split(",");
			for(String label : labels){     	

				//2 increment #(Y=y) by 1  
				/* if hashtable already has this key*/
				if(CountsHash.containsKey("0" + label)){
					CountsHash.put("0" + label, CountsHash.get("0" + label) + 1);
				}
				/* if not has this key */
				else{
					
					CountsHash.put("0" + label,1);
					StringBuffer temp = new StringBuffer();
					temp.append("0L\t").append(label);
					//String temp = "0L"+ "\t" + label;
					/* 6 put in this to count the number of labels */
					CountsHash.put(temp.toString(),1);   
				}  	  

				//1 increment #(Y=*) by 1;  
				totalDocs++;

				//4 increment #(Y=y, W=*) by N  
				StringBuffer yW_key = new StringBuffer();
				yW_key.append("0W\t").append(label);
				//String yW_key = "0W" + "\t" + label;
				if(CountsHash.containsKey(yW_key.toString())){
					CountsHash.put(yW_key.toString(), CountsHash.get(yW_key.toString()) + sTokens.size());
				}else{
					CountsHash.put(yW_key.toString(),sTokens.size());	
				}  	  

				for(int i=0;i<sTokens.size();i++)
				{						
					//3 increment #(Y=y,W=wi) by 1    
					StringBuffer yw_key =new StringBuffer();
					yw_key.append(label).append('\t').append(sTokens.get(i));
					//String yw_key = label + "\t" + sTokens.get(i);
					if(CountsHash.containsKey(yw_key.toString())){
						CountsHash.put(yw_key.toString(), CountsHash.get(yw_key.toString()) + 1);
					}else{
						CountsHash.put(yw_key.toString(),1);      					
					}

					//5 count the size of vocabulary 
					StringBuffer unique_word =new StringBuffer();
					unique_word.append("0V\t").append(sTokens.get(i));
					//String unique_word = "0V" + "\t" + sTokens.get(i);
					if(! CountsHash.containsKey(unique_word.toString())){

						CountsHash.put(unique_word.toString(),1);      					
					}   		  
				}
				
				//If hashtable is larger than a certain size, clear*/
				if(CountsHash.size() > 100000) {

					PrintCounts();
					CountsHash.clear();
				}
			}          
		}
		/* Output to stdout */
		PrintCounts();
		System.out.println("totalDoc"+"\t\t"+totalDocs);
	}

}