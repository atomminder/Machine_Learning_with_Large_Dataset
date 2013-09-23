/**
 *	10605 BigML
 *	Assignment5: Logistic Regression Using Stochastic Gradient Descent
 *	Andrew id: qxi
 *	@author Qiangjian(Jason) Xi
 *  
 *  This class stores global parameters
 *
 */
public class globalParameters {

	static double mu = 0.0001;
	static double eta = 0.5;
	static long vocalbulary = 100000;

	static String [] AllLabels = 
		{"nl","el","ru","sl","pl","ca","fr","tr","hu","de","hr","es","ga","pt"};

	static int findLabelIndex(String input) {

		int index = 0;
		for(int i=0; i < AllLabels.length; ++i) {
			if(AllLabels[i].equals(input)) {
				index = i;
			}
		}
		return index;
	}

	public static void main(String[] args) {

		System.out.println(findLabelIndex("pt"));
	}

}