package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class MapperServer extends UnicastRemoteObject implements MapperService {
	/**
	 * This is the Mapper class. It exnteds UnicastRemoteObject and implements MapperService interface.
	 * This remote object gets a request from master, and do the mapper's task.
	 */
	private static long pid = ProcessHandle.current().pid();		// Process id.
	static FileManager writer = new FileManager();		// Call FileManager.
	private static List<List<Integer>> partitionIndices = new ArrayList<List<Integer>>();		// store partition indices splitted by lines in the input file

	protected MapperServer() throws RemoteException {
		super();
	}		
	
	@Override
	public List<String> masterLaundchMapper(String appClassPath,String filePath, List<Integer> indices, int N) throws RemoteException {
		/* 
		 * The master will call this method to reqeust the mapper doing the map task. 
		 * The master passes the partition path, then it splits the partition.
		 * The mapper then finds the user-defined function, and invokes the functions on each row of the partition.
		 */
	    String currentDirectory = System.getProperty("user.dir"); 		// get a current directory

		List<?> splittedPartition = readPartialFile(currentDirectory+"/"+filePath, indices.get(0), indices.get(1));		// read the partition
		List<String> intermediateFilePath = doMap(splittedPartition, appClassPath, N);		// call the function that works map
		
		return intermediateFilePath;		// return the intermediate file location.
//		throw new RemoteException();
	}
	
	@SuppressWarnings("unchecked")
	private List<String> doMap(List<?> splittedPartition, String appClassPath, int N) {
		/*
		 *  Here is where Mapper does its job 
		 * 	it finds the user-define map functions and invokes it on each row of partition.
		 * */
	    String currentDirectory = System.getProperty("user.dir"); 		// get a current directory

		String basicIntermediateFilePath = currentDirectory+"/"+appClassPath+ "/intermediate_file"+String.valueOf(pid); 		// create a intermediate with its id."
		List<String> intermediateFilePath = new ArrayList<String>();
				
		Class clazz = null;		//create Class object
		Method map = null; 		//create Method object
		List<Map<?,?>> result = new ArrayList<>();  // expects user map function returns List<Map<T,S>>
		Map<Integer,List<Map<?,?>>> regions = new HashMap<>();
		
		try {
			clazz = Class.forName(appClassPath);		//look for a class name with user application 
			Object obj = clazz.getDeclaredConstructor().newInstance();		//create an object of the class
			Method methods[] = clazz.getDeclaredMethods();		//finds all methods in the user class 
			for (Method method : methods) {			//look for a map function
				if(method.getName() == "map") {
					map = method;
					break;
				}
			}
			
			if(map == null) {
				// if the Mapper cannot find the method, it will exit the program
				System.out.println("[Mapper("+String.valueOf(pid)+")]" + "Can't find the user map function,\nPlease Check the map function name.");
				System.exit(1);
			}
			

			for (Object row: splittedPartition) {
				List<Map<?,?>> temp = (List<Map<?, ?>>) map.invoke(obj, null,row);  // create a List of pairs for the map
				result.addAll(temp);
			}
			
			regions = partitionRegion(result, N);
			
			for (int region_number: regions.keySet()) { // write the intermediate file per region.
				String intermediateFile = basicIntermediateFilePath + "_"+region_number+".txt";
				for (Map<?,?> pair: regions.get(region_number))
					pair.forEach((key, value) -> writer.writeFile(intermediateFile,"("+key.toString()+","+value.toString()+")", true));
				intermediateFilePath.add(intermediateFile);
			}
			

		} catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		
		return intermediateFilePath; // return the locations of intermediate File Path.
	}
	
	public static Map<Integer, List<Map<?, ?>>> partitionRegion(List<Map<?,?>> list, int N) {
		/*
		 * This is to partition each intermediate file into R regions.
		 * For simplicity, set R = N.
		 * Each mapper creates N intermediate files(regions), so eventually N * N intermediate files exist.
		 */
		Map<Integer,List<Map<?,?>>> partitionPairs = new HashMap<>();
		
		for(Map<?,?> pair: list) {
			pair.forEach((key, value)-> {
				List<Map<?, ?>> tempList = new ArrayList<>();
				Map<Object,Object> temp = new HashMap<>();
				temp.put(key, value);
				int hashValue = keyHash(key.toString(),N);
				
				tempList.add(temp);
				if (partitionPairs.get(hashValue) != null)
					tempList.addAll(partitionPairs.get(hashValue));
				
				partitionPairs.put(hashValue, tempList);
				
			});
		}
		System.out.println("partitionPairs: "+partitionPairs);   // just for debug
		return partitionPairs;
	}
	
	public static int keyHash(String key, int N) {
		/*
		 * It at least group keys with the same values together.
		 * With the same key, it will eventually put in the same intermediate file.
		 * It uses djb2 hash function algorithm.
		 */
		int hash = 5381;
		for (int i=0; i<key.length(); i++) {
			hash = hash * 33 + key.charAt(i);
		}
		
		return Integer.remainderUnsigned(hash, N);	
		//return hash % N;
	}


	public static List<?> readPartialFile(String filepath, int start, int end){
		/*
		 * this is to read from a given start line to a given end line in the input file.
		 */
		
		List<Object> partition = new ArrayList<>();  // to store each line of the partition
		
		File inputFile = new File(filepath);	//	read the input file
		Scanner scanner = null;
		try {
			scanner = new Scanner(inputFile);	// scanner for the input file
			int i = 0;
			while(scanner.hasNextLine() && i < start) {	
				// skip lines until it gets to the start line index
				scanner.nextLine();
				i++;
			}
			while(scanner.hasNextLine() && i < end) {
				// read lines until it gets to the end line index or has no line
				String line = scanner.nextLine();
				partition.add(line);
				i++;
			}
		}catch (FileNotFoundException e) {
			e.printStackTrace();

		}finally {
			scanner.close();
		}
		//System.out.println(inter_restore);		// only for debug
		return partition;
	}
	
	public static void main(String args[]) throws RemoteException {
		// This is Mapper remote object main.
		try {
			
			int port = Integer.valueOf(args[0]);
			MapperServer map = new MapperServer();	
			Registry registry = LocateRegistry.createRegistry(port);// create registry with port number
			registry.rebind("map", map);		// bind the remote object with "map"	

		}catch(Exception e) {
			e.printStackTrace();
		}	
		
		new MapperServer();
	}

}

