import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.rmi.NotBoundException;

import mapreduce.*;

public class CountSameLengthWords implements Mapper<Integer, String>, Reducer<Integer> {
	/*
	 * From this application, we can figure out how many words of certain lengths exist.
	 */
	
	
	@Override
	public List<Map<Integer, String>> map(Object key, String value) {
	/*
	 * (K1,V1) => list(K2,V2)
	 * (Deer Bear River) => (4,Deer, 4,Bear, 5,River)
	 */
					
		// ----- Read Partition and Recognize Words ------
		value = value.toUpperCase();					// unify to capital letters
		value = value.replaceAll("[^a-zA-Z0-9]", " ");    	// Remove special characters
		String[] words = value.split(" ");
			
		// ----- Output the Length of the Word as the Key and the Words Itself as the Value ------
		List<Map<Integer, String>> map_result = new ArrayList<>();	    // key-value pair list which can allow duplicated key.
		for (int j=0; j<words.length; j++) {
			if (!words[j].isBlank()) {
				Map<Integer, String> wordlength = new HashMap<Integer, String>();    // declare new key-value pair for every loop
				wordlength.put(words[j].length(), words[j]);
				map_result.add(wordlength);
				//System.out.println("wordlength: " + wordlength);
			}

		}
		//System.out.println("map_result: " + map_result);
			
		return map_result;    // return the map function result
	}
	


	@Override
	public List<Integer> reduce(String key, List values) {
	/*
	 * (K2,list(V2)) => list(V2)
	 * (4,[Deer,Bear]) => [2]
	 */
						
		// ------ Count the Number of Words which have already grouped by the Same Length --------
		
		List<Integer> reduced_value = new ArrayList<Integer>();
		
		reduced_value.add(values.size());
		
		return reduced_value;
	}

	public static void main(String args[]) throws NotBoundException, IOException {
		MapReduce.main(new String[] {"CountSameLengthWords/config.properties"});		// call mapreduce main passing the configuration file path
	}


}