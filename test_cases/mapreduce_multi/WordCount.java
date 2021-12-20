
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.rmi.NotBoundException;

import mapreduce.*;

public class WordCount implements Mapper<String,Integer>, Reducer<Integer> {	
	
	@Override
	public List<Map<String, Integer>> map(Object key, String value) {
	/*
	 * (K1,V1) => list(K2,V2)
	 * (Deer Bear River) => (Deer,1, Bear,1, River,1)
	 */
					
		// ----- Read Partition and Recognize Words ------
		value = value.toUpperCase();					// unify to capital letters
		value = value.replaceAll("[^a-zA-Z0-9]", " ");    	// Remove special characters
		String[] words = value.split(" ");
			
		// ----- Map Each Word into (Word, 1) key-value pairs ------
		List<Map<String, Integer>> interwords_list = new ArrayList<>();	    // key-value pair list which can allow duplicated key.
		for (int j=0; j<words.length; j++) {
			if (!words[j].isBlank()) {

				Map<String, Integer> interwords = new HashMap<String, Integer>();    // declare new key-value pair for every loop
				interwords.put(words[j], 1);
				interwords_list.add(interwords);
				//System.out.println("interwords: " + interwords);		// only for debug
			}

		}
		//System.out.println("interwords_list: " + interwords_list);		// only for debug
			
		return interwords_list;    // return the map function result
	}

	@Override
	public List<Integer> reduce(String key, List values) {
	/*
	 * (K2,list(V2)) => list(V2)
	 * (Bear,(1,1)) => (2)
	 */
						
		// ------ For each words, increase its count --------
		List<Integer> reduced_value = new ArrayList<Integer>();
		int sum = 0;
		for (int i=0; i<values.size(); i++)
			sum += Integer.parseInt(values.get(i).toString());
		reduced_value.add(sum);
		
		return reduced_value;
	}

	public static void main(String args[]) throws NotBoundException, IOException {
		MapReduce.main(new String[] {"WordCount/config.properties"});		// call mapreduce main passing the configuration file path
	}

}