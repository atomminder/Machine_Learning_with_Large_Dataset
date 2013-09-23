/*
 * Calculate the accuracy
 * andrew id: qxi
 * name: Qiangjian Xi 
 * 
 */
import java.io.*;


public class Accuracy {

	/* main */
	public static void main(String[] args) {

		try {
			
			FileInputStream fstream = new FileInputStream("results/predict_results/part-r-00000");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String line;
			long flag = 0;
			long total = 0;
			/* read each line */
			while ((line = br.readLine()) != null)   {
				System.out.println(line);
				total ++;
				if(line.charAt(line.length() - 1) == 'h')
					flag++;
			}
			/* calculate */
			System.out.println("Total: " + total + "\t Hit: " + flag);
			System.out.println("Accuracy: " + ((double)flag / total));
			in.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
