import java.io.*;

/* step3: create messages */
public class Message{

	public static void main(String args[]) throws IOException{

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String inLine = null;

		/* every line */
		while( (inLine = in.readLine()) != null && inLine.length() != 0 ) {
			
			String[] words = inLine.split("\t");

			String[] temp = words[0].split(" ");
			
			
			if(temp.length > 1) {

				StringBuffer output = new StringBuffer();
				output.append(temp[0]).append('\t').append(words[0]);
				System.out.println(output);
				//output.delete(0,output.length()-1);
				output.setLength(0);
				output.append(temp[1]).append('\t').append(words[0]);
				System.out.println(output);
			}


		}
	}
}