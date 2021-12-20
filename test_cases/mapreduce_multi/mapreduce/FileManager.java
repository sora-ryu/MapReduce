package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileManager {

	public static List<Map<?,?>> readFile(String filepath) throws IOException {
	/*
	 * Read the file and return the whole content of the file
	 */
		
		List<Map<?,?>> inter_restore = new ArrayList<>();	    // key-value pair list which can allow duplicated key.
		
		BufferedReader br = new BufferedReader(new FileReader(filepath));
		try {
			String line = br.readLine();
			while (line != null) {
				
				Map<String,String> temp = new HashMap<>();     // declare new key-value pair for every loop
				String[] key_value = line.split(",");
				
				key_value[0] = key_value[0].replace("(", "");		// clean the key values
				key_value[1] = key_value[1].replace(")", "");
				temp.put(key_value[0], key_value[1]);

				inter_restore.add(temp);
				line = br.readLine();
			}
		} finally {
			br.close();
		}
		//System.out.println(inter_restore);		// only for debug
		return inter_restore;
	}
	

	public void writeFile(String filepath, String content, boolean overwrite) {
	/*
	 * Write the content in a file. Can overwrite if it's true
	 */
		try {
			File file = new File(filepath);		// open the file
			
			if (!file.exists()) {
				file.createNewFile();		// if there is no such file, create new one
			}
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile(), overwrite);		// create a file writer
			BufferedWriter bw = new BufferedWriter(fw);		//create a buffer writer
			bw.write(content);			// write the contents 
			if (overwrite)	
				bw.newLine();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}