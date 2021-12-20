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
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class MapReduce implements Runnable{


	private static long masterPID = ProcessHandle.current().pid();
	
	/* user inputs  */
	private static String appClassPath;
	private static String inputFilePath;
	private static String outputFilePath;
	private static int N;
	private static String configFilePath;

	/* multi workers variables*/
	private static List<Process> processList = new ArrayList<Process>(); // Process List
	private static List<String> portList = new ArrayList<>(); 	// store port numbers
	private static boolean isMapDone = false; 	// indicate if mappers are done
	
	/* partitioning variables */
	private static List<List<Integer>> partitionIndices = new ArrayList<List<Integer>>();		// store partition indices splitted by lines in the input file
	private static Map<Integer,List<String>> intermediate_locationsByRegion = new HashMap<>();

	@Override
	public void run() {
		/*
		 * What is executed by the thread after calling start()
		 */
		Thread t = Thread.currentThread();

		String port = portList.get(0);		// get ports connected worker Servers
		portList.remove(0);		// remove the port to prevent other worker use the port

		if(!isMapDone) {
			/*
			 * If Mappers need to work, each thread will send requests to mapper servers and get results
			 */
			List<Integer> indices = partitionIndices.get(0);	// get indices of a partition
			partitionIndices.remove(0);		// remove the taken partition from the partition indices list

			String url = "rmi://localhost:"+port+ "/map";
			MapperService mapper;
			try {
				mapper = (MapperService)Naming.lookup(url);
				List<String> location = mapper.masterLaundchMapper(appClassPath,inputFilePath,indices, N);
				storeIntermdeiateFileByRegion(location);
			} catch (MalformedURLException | RemoteException | NotBoundException e) {
				e.printStackTrace();
			}		
		}else {
			/*
			 * 	If reducers need to work, each thread will send requests to reduecer servers
			 */

			String url = "rmi://localhost:"+port+ "/reduce";
			ReducerService reducer;
			try {
				List<String> intermediate_locations = intermediate_locationsByRegion.get((int) (t.getId()%N)); // get the locations stored by region
				if(intermediate_locations != null) {
					reducer = (ReducerService)Naming.lookup(url);	// look for the reducer server 
					reducer.masterLaunchReducer(intermediate_locations, appClassPath, outputFilePath);	// sends requests 
				}
			} catch (MalformedURLException | RemoteException | NotBoundException e) {
				e.printStackTrace();
			} 

		}
	}

	private static void storeIntermdeiateFileByRegion(List<String> locations){
		/*
		 This is to sotre intermediate files by region in order to send each reducer different files
		 */
		String location = null;
		for(int i = 0; i < locations.size(); i++) {
			location = locations.get(i);
			List<String> temp;
			/* 
			   If the dictionary already has the region as a key, it added the location into the region.
			   Otherwise, it creates a new list.
			*/
			if(intermediate_locationsByRegion.containsKey(i)){
				temp = intermediate_locationsByRegion.get(i);

			}else {
				temp = new ArrayList<>();
			}
			temp.add(location);
			intermediate_locationsByRegion.put(i, temp);
		}
	}

	private static void launchMapper() throws NotBoundException, InterruptedException {
		/**
		 * This is a function to launch mappers by the master.
		 * It will Create a process and exectue Mapper class
		 * The Mapper class will create a registry for RMI
		 * The, Master will send Mapper the input file location and get locations of intermediate files.
		 */
	    String currentDirectory = System.getProperty("user.dir"); 		// get a current directory
		Process p = null;		// a worker process

		for (int i = 0; i < N; i++) {
			int port = 1099; 	// starting port number
			try {
				port += i;		// generate a port number by adding 1
				p = Runtime.getRuntime().exec("java -jar MapperServers.jar " + String.valueOf(port) , null, new File(currentDirectory)); 		// create a worker process then executes mapper server
				processList.add(p);			// add the process in the process list
				System.out.println("[Master("+String.valueOf(masterPID)+")] created a mapper("+String.valueOf(p.pid())+")");		// display status
				portList.add(String.valueOf(port));

				Thread.sleep(1000);			// wait until that the Mapper class creates a registry

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void assignWorkerTask() {
		/*
		 * This is a method that create child threads and start them and join them.
		 */
		ArrayList<Thread> threads = new ArrayList<>(); 		// Thread list

		for (int i = 0; i<N; i++) {
			// Create N child threads
			Thread t = new Thread(new MapReduce());
			t.start();
			threads.add(t);
		}

		for(int i = 0; i<threads.size(); i++) {
			// join
			Thread t = threads.get(i);
			try {
				t.join();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}

		System.out.println("[Master("+String.valueOf(masterPID)+")] Workers finished their jobs." );		// display status
	}


	private static void killWorkers() {
		/*
		 * After all workers are done with their jobs, the master will kill them
		 *
		 * */

		for(int i = 0; i < processList.size(); i++) {
			System.out.println("[Master("+String.valueOf(masterPID)+")]" + " killed a worker(" + String.valueOf(processList.get(i).pid())+")");
			processList.get(i).destroy();	 // kill the process
		}
		processList.clear();		//remove the process in the process list

	}

	private static void laundchReducer(int N) throws NotBoundException, InterruptedException {
		/*
		 * This is a function to launch reducers by the master
		 * like mappers, we should handle cocurrency here as well.
		 *
		 * */
		Process p;		// a worker process

		if(!processList.isEmpty()) {
			// if there are still mapper processses alive the master kill them, kill all again.
			killWorkers();
		}

	    String currentDirectory = System.getProperty("user.dir"); 		// get a current directory

		for (int i = 0; i < N; i++) {
			int port = 1098;		//starting port number
			try {
				port -= i;		// generating a port number by reducing 1
				p = Runtime.getRuntime().exec("java -jar ReducerServers.jar " + String.valueOf(port) , null, new File(currentDirectory)); 		// create a worker process then executes mapper server
				processList.add(p);			// add the process in the process list
				System.out.println("[Master("+String.valueOf(masterPID)+")] created a reducer("+String.valueOf(p.pid())+")");		// display status
				portList.add(String.valueOf(port));

				Thread.sleep(1000);			// wait until that the reducer class creates a registry

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	private static void  splitInput() {
		/*
		 * This is a function to split the partition.
		 * Firstly, it splits the partition by line and add them into a List.
		 * Then it evenly groups them into N number of partitions.
		 * */


		File inputFile = new File(inputFilePath);		//get the input file by the file path
		List<String> lines = new ArrayList<String>(); 		//create a list
		Scanner scanner = null;
		try {
			scanner = new Scanner(inputFile);		// scanner for input file
			while(scanner.hasNextLine()) {		// get line by line
				String line = scanner.nextLine();
				lines.add(line);		// add each line into the list.
			}

			// If there's 6 lines in input file and N = 4,
			// divide a list which has 6 lines into lists which have 2 lines, 2 lines, 1 line, 1 line respectively.
			partitionIndices = partitioning(lines);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally {
			scanner.close();
		}
	}
	private static List<List<Integer>> partitioning(List<?> list) {
		/*
		This is to partition the input file, and return the list of pairs of start line index and end line index of each parition.
		*/
		
		List<List<Integer>> partitionIndices = new ArrayList<List<Integer>>();

		int remainder = list.size() % N;
		int basicPartitionSize = list.size() / N;
		int offset = 0;

		for (int i=0; i<N; i++) {
			List<Integer> startEndpairs = new ArrayList<Integer>();
			if (remainder > 0) {  // The remainder exists, it will be additionally shared at the beginning.
				startEndpairs.add(i*basicPartitionSize+offset);
				startEndpairs.add((i+1)*basicPartitionSize+offset+1);
				remainder--;
				offset++;
			} else {
				startEndpairs.add(i*basicPartitionSize+offset);
				startEndpairs.add((i+1)*basicPartitionSize+offset);
			}
			partitionIndices.add(startEndpairs);
		}

		return partitionIndices;
	}

	public static void master() {
		/*-----------Master task----------------------------------*/
		System.out.println("[Master("+String.valueOf(masterPID)+")] Started working."); // print Master is up.
//		doMaster(); // call the Master
		try {
			//	intermediate_locations.add(launchMapper());		// launch Mappers and get locations of intermediate files
			launchMapper();
			assignWorkerTask();
			isMapDone = true;
			killWorkers();	 // after Mappers are done, kill the processes
//			System.out.println(intermediate_locations); // just for debug.
			laundchReducer(N);		// launch Reducers
			assignWorkerTask();
			killWorkers();		// after Reducer are done, kill the processes
		} catch (Exception e) {
			e.printStackTrace();

		}
		System.out.println("[Master("+String.valueOf(masterPID)+")] Completed the job."); // print Master is up.

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

		/*----------- split the input files -----------------------*/
		splitInput();

		/*----------- execute Master-------------------------------*/
		master();
	}

}
