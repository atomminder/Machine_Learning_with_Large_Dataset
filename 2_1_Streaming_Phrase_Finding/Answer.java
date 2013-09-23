import java.io.*;


/* step4: 
 * 
 * input:
 * messages + unigram frequency file
 * (Use a secondary key so the attribute-value pairs come first, 
 * and the messages come last, as described in lecture)
 * 
 * */
public class Answer {


	public static void main(String args[]) throws IOException{

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String inLine = null;
		String previous= "-";
		long bgCount = 0;
		long fgCount = 0;

		/* every line */
		while( (inLine = in.readLine()) != null && inLine.length() != 0 ) {

			String words[] = inLine.split("\t");
			/* If this key does not occur before */
			if(words[0].compareTo(previous) != 0) {

				String[] temp1 = words[1].split(" ");
				bgCount = Long.parseLong(temp1[1]);
				String[] temp2 = words[2].split(" ");
				fgCount = Long.parseLong(temp2[1]);				


				previous = words[0];

			}
			/* If this key occurs before, increase the corresponding count */
			else {

				StringBuffer output = new StringBuffer();
				String[] temp3 = words[1].split(" ");
				/* If the key is the first words of the two-word phrase */
				if(temp3[0].compareTo(words[0]) == 0) {
					
					/* Output format: two-word-phrase \t Bx count \t Cx count */
					output.append(words[1]).append('\t').append("Bx ").append(bgCount);
					output.append('\t').append("Cx ").append(fgCount);
					System.out.println(output);
					temp3[0] ="-";
				}
				/* If the key is the second words of the two-word phrase */
				else {

					/* Output format: two-word-phrase \t By count \t Cy count */
					output.append(words[1]).append('\t').append("By ").append(bgCount);
					output.append('\t').append("Cy ").append(fgCount);
					System.out.println(output);

				}
				output.setLength(0);

			}


		}
	}
}