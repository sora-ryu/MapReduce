# MapReduce

### Jiye Choi, Sora Ryu, Chiehmin Chung

- This is a MapReduce Implementation code on a single server. Even though it works on a single server, it mimics the behavior of a distributed implemenation by using Remote Method Invocation. A user will pass 'configuration file' including application name, an input txt file location, location for output files, and the number of workers, N. 

## Master-Slave Architecture 
### Master  
-    Before the master starts its job, it will split the user input file into partitions based on N. Then, the master will creates N processes to invoke Mapper Servers. Each Mapper server has its own port number for registry. To call the mapper sever's methods, the master creates N threads. Threads send requests to the mapper servers, and get intermediate file locations including a region number at the end of file name. Before the master pass the locations to Reducers, it kills current processes. As the master launches mappers, the master creates N processes and invoke Reducer Servers. Like the mapper server, each reducer server has its own port number for registry. N threads send request Reducer Server with the intermediate files. Each reducer server is responsible for all intermediate files of each region number. After the reducers complete their jobs, the master kill the processes and terminate the program.

### Mapper 
-    A mapper gets a partition location (indice of a start line and an end line) from the master. Then, it splits the partition and invokes a user-defined map function on each row of the partition. The mapper gets the user-defined map function's output and it splits all outputs into N regions by using simple hash-based partitioning algorithm. (In order to make each reducer has something to work on, we just simply set R = N.) Then, it writes them on intermediate files named 'intermediate_file[the mapper pid]_[region number]'. Finally, it returns the file locations to the master.
-    Overall, since there are N mappers, eventually N * N intermediate files exist.

### Reducer 
-    A reducer gets multiple intermediate file locations from the master. Then, it reads and store data from all assigned intermediate files, and shuffles and sorts the files by key. On each key, it invokes a user-defined reduce function. Finally, the reducer writes the outputs in a output file.
-    Overall, each reducer reads N intermediate files which contain specific region number. Thus, all intermediate files are covered, and not even duplicated. Also, since there are N reducers, eventually N output files exist.

### Fault Tolerance 
-   The master detects a worker's failures. When one of worker servers (either mapper or reducer) fails, the server throws 'RemoteException'. Then, the exception is caught by the master, and the master relaunches a new process. This new process worker works on the part that the faulty worker was responsible. 
- In order to test the fault tolerance, we intentionally introduce a fail on a random mapper and reducer after we launch mappers/reducers. It happens in the library code (mapreduce class).

## Test cases 
-  there are three test cases ; a single process, multiple processes, and multiple process with fault tolerance. 
-  If you want to test all cases at the same time, please run the single test script (test.sh) in the root folder.
-  if you want to test the cases separately, run each test script in test_scripts.
-  All test cases are in test_cases folder.

## How to use
- First of all, please make sure that your application class file, MapperServers.jar, ReducerServers.jar are in the 'src' folder.
- ** the jar files compiled by JDK 15. please make sure you installed JDK 15 or newer**
-       (e.g., MapReduce/src/WordCount.java)
- Your application class should implement 'Mapper' and 'Reducer' interfaces to implement map, reduce functions.
- We recommend you to make a new folder which has exactly the same name with application class file, and save configuration file and input file there
- In your configuration file (config.properties) , you must provide your application's name, input file path, output file path, and the number of workers you wish.
- Lastly, your application should call the MapReduce.main passing the configuration file path. 
-       (e.g.,MapReduce.main(new String[] {"application's name/config.properties"});)
- Then, add these command lines in the single run script (test.sh) as shown below
-       javac [your application].java
-       java [your application]
- Finally, run the command line in the root folder.
-       bash test.sh

## Test Applications
If you run this code by default, it automatically compiles the code and runs all of the tests and experiments. Each application will be executed in three different behaviors; with a single process, with multiple processes, and with multiple processes and one fault. The brief information of each test application is as follows.
* Word Count
*       To count how many times each word is occured in a input file.
* Count the same length words
*       To count the same length of words in a input file.
* Count Vowels
*       To count the same number of vowels of words in a input file.
