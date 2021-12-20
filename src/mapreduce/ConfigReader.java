package mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class ConfigReader {
	/*
	 * This class is to read the user's configuration file 
	 */
	
	InputStream is;  	// declare an inputstream object
	String path; 		
	
	ConfigReader(String path){
		this.path = path;	
	}
	public HashMap<String,String> getProperties() throws IOException{
		
		/*
		 * this function is to get the property values from the configuration file.
		 * it returns the HashMap with values 
		 */
		
		HashMap<String,String> hmap = new HashMap<>();
		
		try { 
			Properties properties = new Properties();		// create propeties pbject
			String propFileName = path;		// set the config.properties file path 
			is = getClass().getClassLoader().getResourceAsStream(propFileName);		// get the file contents 
			
			if(is!=null) {
				// if the file exits it will load to the properties 
				properties.load(is);
			}else {
				// otherwise throw an exception
				throw new FileNotFoundException("property file " + propFileName + " not found in the classpath");
			}
			
			// Put all the values with the key 
			hmap.put("application", properties.getProperty("application"));
			hmap.put("input", properties.getProperty("input"));
			hmap.put("output", properties.getProperty("output"));
			hmap.put("N", properties.getProperty("N"));
						
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			is.close();		// close it
		}
		
		return hmap;		// return the hash map	
	}
}
