import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;




/* Step1,2: Counter the xy -> Cxy, xy -> Bxy */
public class BiCounter {




	public static void main(String args[]) throws IOException{


		/* System.in */
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String inLine = null;
		String previous= "-";
		long bgCount = 0;
		long fgCount = 0;

		/* every line */
		while( (inLine = in.readLine()) != null && inLine.length() != 0 ) {

			String[] words = inLine.split("\t");


			/* If this key does not occur before */
			if(words[0].compareTo(previous) != 0) {

				if(previous.compareTo("-") != 0) {

					StringBuffer temp = new StringBuffer();
					temp.append(previous).append('\t').append("Bxy ").append(bgCount);
					temp.append('\t').append("Cxy ").append(fgCount);
					System.out.println(temp);

				}
				/* initialize the count */
				bgCount = 0;
				fgCount = 0;
				/* Increase the count */
				int temp1 = Integer.parseInt(words[2]);
				if(Integer.parseInt(words[1]) >= 1990) {
					fgCount += temp1;
				}
				else
					bgCount += temp1;
				previous = words[0];

			}
			/* If this key occurs before, increase the corresponding count */
			else {

				int temp2 = Integer.parseInt(words[2]);
				if(Integer.parseInt(words[1]) >= 1990) {
					fgCount += temp2;
				}
				else
					bgCount += temp2;
			}





		}
		StringBuffer temp = new StringBuffer();
		temp.append(previous).append('\t').append("Bxy ").append(bgCount);
		temp.append('\t').append("Cxy ").append(fgCount);
		System.out.println(temp);


	}
}
