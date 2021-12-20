package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class MapReduce {
	
	
	private static List<Process> processList = new ArrayList<Process>(); // Process List
	private static long masterPID = ProcessHandle.current().pid();
	private static String appClassPath;
	private static String inputFilePath;
	private static String outputFilePath;
	private static int N;
	private static String configFilePath;
	
	private static void doMaster() {
		/*
		 * This is the master function. 
		 * 
		 * 1) the master will launch mappers with partitions
		 * 2) then, wait for mappers, make sure all of mappers are done
		 * 3) kill the processes
		 * 4) then, launch reducers with the locations of intermedicate files from mappers 
		 * 5) kill the processes 
		 * 6) indicate the user the task is done
		 * 
		 * */
				
		List<String> intermediate_locations = new ArrayList<String>();		//create a list to store the intermedicate file locations

		try {
			intermediate_locations.add(launchMapper());		// launch Mappers and get locations of intermediate files 
			killWorkers();	 // after Mappers are done, kill the processes		
			laundchReducer(N,intermediate_locations);		// launch Reducers
			killWorkers();		// after Reducer are done, kill the processes
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	private static String launchMapper() throws NotBoundException, InterruptedException {
		/**
		 * This is a function to launch mappers by the master.
		 * It will Create a process and exectue Mapper class
		 * The Mapper class will create a registry for RMI 
		 * The, Master will send Mapper the input file location and get locations of intermediate files.
		 */
	    String currentDirectory = System.getProperty("user.dir"); 		// get a current directory
		Process p = null;		// a worker process
		String url = "rmi://localhost:1099/map";		// url for the remote host
		String location = null;

		try {
			p = Runtime.getRuntime().exec("java -jar MapperServer.jar", null, new File(currentDirectory)); 		// create a worker process then executes mapper server
			processList.add(p);			// add the process in the process list 
			System.out.println("[Master("+String.valueOf(masterPID)+")] created a mapper("+String.valueOf(p.pid())+")");		// display status
			
			Thread.sleep(1000);			// wait unitl that the Mapper class creates a registry 
			
			MapperService mapper = (MapperService)Naming.lookup(url);		// lookup the remote object with the url
			location= mapper.masterLaundchMapper(appClassPath,inputFilePath);		// the Master pass the mapper the inputfile path, get intermediate file locations.

		} catch (IOException | NotBoundException e) {
			e.printStackTrace();
		} 

		return location; // return locations
	}

	private static void killWorkers() {
		/*
		 * After all workers are done with their jobs, the master will kill them
		 * 
		 * */
		
		for(int i = 0; i < processList.size(); i++) {
			System.out.println("[Master("+String.valueOf(masterPID)+")]" + " killed a worker(" + String.valueOf(processList.get(i).pid())+")");
			processList.get(i).destroy();	 // kill the process
			processList.remove(i);		//remove the process in the process list
		}
	}

	private static List<String> laundchReducer(int N, List<String> intermediate_locations) throws NotBoundException, InterruptedException {
		/*
		 * This is a function to launch reducers by the master 
		 * like mappers, we should handle cocurrency here as well.
		 * 
		 * */
		Process p;		// a worker process 
		List<String> outputLocations = null;	// output locations 
		String url = "rmi://localhost:1098/reduce";
		
		if(!processList.isEmpty()) {
			// if there are still mapper processses alive the master kill them, kill all again.
			killWorkers();
		}
		
	    String currentDirectory = System.getProperty("user.dir"); 		// get a current directory
		try {
			p = Runtime.getRuntime().exec("java -jar ReducerServer.jar", null, new File(currentDirectory)); // execute Mapper Class by the child process.
			processList.add(p); // add the process into the process list 
			System.out.println("[Master("+String.valueOf(masterPID)+")] created a reducer("+String.valueOf(p.pid())+")");

			Thread.sleep(1000);		// wait unitl that the Mapper class creates a registry 

			ReducerService reducer = (ReducerService)Naming.lookup(url); // to find a ref of remote object 
			reducer.masterLaunchReducer(intermediate_locations, appClassPath, outputFilePath);		// pass reducer the intermediate file locations

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return outputLocations; // return the output locations
	}

	public static void main(String[] args) throws NotBoundException, IOException {
		
		/*-----------Getting Configuration properties -------------*/
		configFilePath = args[0];
		ConfigReader configReader = new ConfigReader(configFilePath);		// call a configuration file reader
		HashMap<String,String> properties  = configReader.getProperties();	// to store pairs of user's configuration properties 
		N = Integer.valueOf(properties.get("N"));		// indicates the number of mappers and reducers
		appClassPath = properties.get("application");		// indicates the application's name
		inputFilePath = properties.get("input");		// indicates the input file's path
		outputFilePath = properties.get("output");	// indicates the output file's path
		
		/*-----------Master task----------------------------------*/
		System.out.println("[Master("+String.valueOf(masterPID)+")] Started working."); // print Master is up.
		doMaster(); // call the Master 
		System.out.println("[Master("+String.valueOf(masterPID)+")] Completed the job."); // print Master is up.

	}
	
	
}
