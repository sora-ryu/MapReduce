import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.rmi.NotBoundException;

import mapreduce.*;

public class Vowels implements Mapper<String,Integer>, Reducer<Integer> {
    /*
      This application is to count how many words have the same number of vowels.
      For instance, words = {'apple', 'audio', 'angel', 'asia'}
                    result = {(2,2),(3,1),(4,1)} since 'apple' and 'angel' have two vowels, and 'audio' has four vowels, and 'asia' has three vowels 
    */
    @Override
    public List<Map<String, Integer>> map(Object key, String value) {


        value.toLowerCase();
        value = value.replaceAll("[^a-zA-Z0-9]", " ");
        String[] words = value.split(" ");

        List<Map<String, Integer>> vowel_list = new ArrayList<>();
        for (int j=0; j<words.length; j++) {
            int vowels = 0;
            if (!words[j].isBlank()) {

                Map<String, Integer> intervow = new HashMap<String, Integer>();
                for (int i=0; i<words[j].length(); i++){
                    char ch = words[j].charAt(i);
                    if (ch == 'a' || ch == 'e' || ch == 'i' || ch == 'o' || ch == 'u') {
                        ++vowels;
                    }

                }
                intervow.put(String.valueOf(vowels), 1);
                vowel_list.add(intervow);
            }

        }
        return vowel_list;
    }

    @Override
    public List<Integer> reduce(String key, List values) {

        List<Integer> reduced_vow = new ArrayList<Integer>();
        int sum = 0;
        for (int i=0; i<values.size(); i++)
            sum += Integer.parseInt(values.get(i).toString());
        reduced_vow.add(sum);

        return reduced_vow;

    }
    public static void main(String args[]) throws NotBoundException, IOException {
      MapReduce.main(new String[] {"Vowels/config.properties"});		// call mapreduce main passing the configuration file path
    }

}
