package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Map.Entry;

public class ReducerServer extends UnicastRemoteObject implements ReducerService{

	/**
	 * This is the Reducer class. It exnteds UnicastRemoteObject and implements ReducerService interface.
	 * This remote object gets a request from master, and do the reducer's task.
	 */
	private static long pid = ProcessHandle.current().pid();		// Process id.
	static FileManager fileManager = new FileManager();		// Call FileManager.
	
	protected ReducerServer() throws RemoteException {
		super();
	}
	
	@Override
	public int masterLaunchReducer(List<String> intermediate_location, String appClassPath, String outputFilePath) throws RemoteException {	
		List<List<Map<?,?>>> intermediate_files = new ArrayList<>(); 	// to store intermediate files 
		
		try {
			for (int i=0; i<intermediate_location.size(); i++) {
				List<Map<?,?>> read = fileManager.readFile(intermediate_location.get(i));	// read the intermediate files
				intermediate_files.add(read);		// add the contents in the list 
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for (int i=0; i<intermediate_files.size(); i++) {
			// to call shuffle and sorts on intermediate files contents 
			Map<String, List<String>> after_sort = shuffleAndSort(intermediate_files.get(i));	
			doReduce(after_sort, appClassPath, outputFilePath);
		}
		return 0; 
	}
	
	public void doReduce(Map<String, List<String>> after_sort, String appClassPath, String outputFilePath) {

		Class clazz = null;
		Method reduce = null;
		List<Map<?,?>> result = new ArrayList<>();  // expects user reduce function returns List<Map<T,S>>

		try {
			clazz = Class.forName(appClassPath);		// find a class with the app class path
			Object obj = clazz.getDeclaredConstructor().newInstance();		// create a new instance of the class 
			Method methods[] = clazz.getDeclaredMethods();		//get all methods 
			for (Method method : methods) {			// look for a reduce method 
				if(method.getName() == "reduce") {
					reduce = method;
					break;
				}
			}
			
			for (Entry<String, List<String>> entry: after_sort.entrySet()) {
				// invoke a user reduce method on each key 
				Map<String,List<?>> temp = new HashMap<>();
				List<?> values = (List<?>) reduce.invoke(obj, entry.getKey(), entry.getValue());
				temp.put(entry.getKey(),values);  // create a List of pairs for the reduce
				result.add(temp); // Concat all returns from reduce 
			}
			for(Map<?,?> pair:result) // write the results into a file 
				pair.forEach((key, value) -> fileManager.writeFile(outputFilePath+"/output"+String.valueOf(pid)+".txt","("+key.toString()+","+value.toString()+")", true));

			
		} catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}

	}
	private static Map<String, List<String>> shuffleAndSort(List<Map<?,?>> intermediate_file) {
		/*
		 * this function is to shuffle and sort the intermediate files 
		 * returns Map (key, values) pairs 
		 */
		
		Map<String, List<String>> result = new HashMap<>();
		
		for (Map<?,?> pair: intermediate_file) {
			for (Map.Entry entry: pair.entrySet()) {
				String key = entry.getKey().toString();		// get key
				if (result.containsKey(key)) {
					List<String> values = result.get(key);		// get all value of the key
					values.add(entry.getValue().toString());
				} else {
					List<String> values = new ArrayList<String>();
					values.add(entry.getValue().toString());
					result.put(key, values);
				}
			}
		}
		
		return result;		// returns sorted pairs 
		
	}
	
	public static void main(String[] args) throws RemoteException{
		try {
			ReducerServer reducer  = new ReducerServer();		// create the reducerSeriveimplement class 
			Registry registry = LocateRegistry.createRegistry(1098);		//registry with port number 
			registry.rebind("reduce", reducer);		//rebind it by 'reduce'
		}catch(Exception e) {
			e.printStackTrace();
		}
		new ReducerServer();
	}

}
