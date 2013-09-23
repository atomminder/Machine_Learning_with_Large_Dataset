/**
 * To count the global things 
 */
import java.io.*;

public class GlobalCounter {

	public static void main(String[] args) {

		BufferedReader br = null;
		try {

			String line;
			br = new BufferedReader(new FileReader("/home/jason/Documents/10605/HW5/bigram_full.txt"));
			long f = 0, b = 0;
			/* iterate */
			while ((line = br.readLine()) != null) {
				
				String[] parts = line.split("\t");
				if(parts[1].equals("1960"))
					f += Long.parseLong(parts[2]);
				else
					b += Long.parseLong(parts[2]);
			}
			System.out.println("f=" + f + ", b=" + b);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}

	}
}

