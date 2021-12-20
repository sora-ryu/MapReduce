package mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
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

	protected MapperServer() throws RemoteException {
		super();
	}		
	
	@Override
	public String masterLaundchMapper(String appClassPath,String filePath) throws RemoteException {
		/* 
		 * The master will call this method to reqeust the mapper doing the map task. 
		 * The master passes the partition path, then it splits the partition.
		 * The mapper then finds the user-defined function, and invokes the functions on each row of the partition.
		 */
	    String currentDirectory = System.getProperty("user.dir"); 		// get a current directory

		List<String> splittedPartition = splitPartition(currentDirectory+"/"+filePath);		// split the partition
		String intermediateFilePath = doMap(splittedPartition, appClassPath);		// call the function that works map
		
		return intermediateFilePath;		// return the intermediate file location.
	}
	
	@SuppressWarnings("unchecked")
	private String doMap(List<String> splittedPartition, String appClassPath) {
		/*
		 *  Here is where Mapper does its job 
		 * 	it finds the user-define map functions and invokes it on each row of partition.
		 * */
	    String currentDirectory = System.getProperty("user.dir"); 		// get a current directory

		String intermediateFilePath = currentDirectory+"/"+appClassPath+ "/intermediate_file"+String.valueOf(pid)+".txt"; 		// create a intermediate with its id."
				
		Class clazz = null;		//create Class object
		Method map = null; 		//create Method object
		List<Map<?,?>> result = new ArrayList<>();  // expects user map function returns List<Map<T,S>> 
		
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
			

			for(String row: splittedPartition) {
				List<Map<?,?>> temp = (List<Map<?, ?>>) map.invoke(obj, null,row);  // create a List of pairs for the map
				result.addAll(temp); // Concat all returns from map 
			}

			for(Map<?,?> pair:result) // write the results into a file 
				pair.forEach((key, value) -> writer.writeFile(intermediateFilePath,"("+key.toString()+","+value.toString()+")", true));
			

		} catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		
		return intermediateFilePath; // return the locations of intermediate File Path.
	}

	private List<String> splitPartition(String filePath) {
		/*
		 * This is a function to split the partition.
		 * For now since N = 1, the mapper's partition = a whole input file.
		 * It splits the partition by line and add them into a List.
		 * 
		 * */
		
		File inputFile = new File(filePath);		//get the input file by the file path
		List<String> splittedPartition = new ArrayList<String>(); 		//create a list 
		
		try {
			Scanner scanner = new Scanner(inputFile);		// scanner for input file
			while(scanner.hasNextLine()) {		// get line by line 
				String line = scanner.nextLine();
				splittedPartition.add(line);		// add each line into the list. 
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return splittedPartition; 		//return partitions
	}
	
	public void quit() throws RemoteException{
		/*
		 * this is to quit the remote object
		 */
		UnicastRemoteObject.unexportObject(this, true);
		
	}
	
	public static void main(String args[]) throws RemoteException {
		// This is Mapper remote object main.
		try {
			MapperServer map = new MapperServer();	
			Registry registry = LocateRegistry.createRegistry(1099);// create registry with port number 9069
			registry.rebind("map", map);		// bind the remote object with "map"	

		}catch(Exception e) {
			e.printStackTrace();
		}	
		
		new MapperServer();
	}

}

