import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class UniGlobalCount {

	public static void main(String[] args) throws IOException {

		String inLine = null;
		long bgWordsCount = 0;
		long fgWordsCount = 0;
		long vocabulary = 0;
		StringBuffer output = new StringBuffer();
		try{
			File file = new File(args[0]);
			FileInputStream fstream = new FileInputStream(file);
			BufferedReader instream = new BufferedReader(new InputStreamReader(fstream));
			while ((inLine = instream.readLine()) != null && inLine.length() !=0){	
				
				String words[] = inLine.split("\t");
				
				vocabulary += 1;
				
				String[] temp1 = words[1].split(" ");
				bgWordsCount += Long.parseLong(temp1[1]);
				String[] temp2 = words[2].split(" ");
				fgWordsCount += Long.parseLong(temp2[1]);	


			}
			instream.close();
			fstream.close();

		}
		catch (Exception e){
			e.printStackTrace();
		}
		output.append("0V ").append(vocabulary).append('\t');
		output.append("B ").append(bgWordsCount).append('\t');
		output.append("C ").append(fgWordsCount).append('\t');
		System.out.println(output);


	}
}