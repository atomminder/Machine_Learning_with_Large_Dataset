package edu.cmu;
/**
 *	10605 BigML
 *  Assignment7: K-Means Clustering with canopy selection on MapReduce
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi
 *	
 */
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleSimilarity {
	
	/*  Jaccard similarity coefficient  */
	public static double similaritySP(String s1, String s2){
		
		List<String> listX = Arrays.asList(s1.split(","));
		List<String> listY = Arrays.asList(s2.split(","));
	
		Set<String> unionXY = new HashSet<String>(listX);
		unionXY.addAll(listY);
		
		Set<String> intersectionXY = new HashSet<String>(listX);
		intersectionXY.retainAll(listY);
		
		double sim = (double)intersectionXY.size() / unionXY.size();
		System.out.println(sim);
		return sim;	
	}
	
	/* 
	 * Given two vectors (same dimensional) A and B, 
	 * Compare the elements of the same location in thesetwo vectors, 
	 * record the number of differences.
	 * For example: vector A = (1,2,3,4,5,6), vector B = (2,2,3,3,5,5), 
	 * according my distance metric,the distance is = 3 
	 * (difference in the 1st, 4th, 6th location).
	 */
	public static double similaritySP2(String s1, String s2){
		
		String[] s1Elements = s1.split(",");
		String[] s2Elements = s2.split(",");
		
		double[] s1ElmInt = new double[s1Elements.length];
		double[] s2ElmInt = new double[s2Elements.length];	
		
		// Change String to double
		for(int i = 0; i < s1ElmInt.length; ++i) {	
			s1ElmInt[i] = Double.parseDouble(s1Elements[i]);
		}
		// Change String to double
		for(int i = 0; i < s2ElmInt.length; ++i) {	
			s2ElmInt[i] = Double.parseDouble(s2Elements[i]);
		}
		double sum = 0;
		for(int i = 1; i < s2ElmInt.length; ++i) {
			
			if (s1ElmInt[i] == s2ElmInt[i]) {
				sum += 1;
			}
			
		}
		return s2ElmInt.length - sum - 1;
	}
}
