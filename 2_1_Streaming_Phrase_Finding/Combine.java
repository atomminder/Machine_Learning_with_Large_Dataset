import java.io.*;

/* step5: merge the data structures */
public class Combine {

	public static void main(String args[]) throws IOException {

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String inLine = null;
		String previous= "-";
		StringBuffer output = new StringBuffer();
		boolean flag = false;

		/* every line */
		while( (inLine = in.readLine()) != null && inLine.length() != 0 ) {

			String words[] = inLine.split("\t");
			/* If this key does not occur before */
			if(words[0].compareTo(previous) != 0) {

				System.out.println(output);
				output.setLength(0);

				/* attain the key */				
				output.append(words[0]).append('\t');

				output.append(words[1]).append('\t');
				output.append(words[2]).append('\t');
				previous = words[0];
				flag = false;

			}
			/* If this key occurs before, increase the corresponding count */
			else {

				output.append(words[1]).append('\t');
				output.append(words[2]).append('\t');
				
				String[] temp = words[1].split("\t");

			}


		}
	}
}