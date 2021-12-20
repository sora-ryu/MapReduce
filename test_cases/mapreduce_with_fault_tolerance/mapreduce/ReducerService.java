package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface ReducerService extends Remote{
	/*
	 * This is the interface of reducer service 
	 */
	public int masterLaunchReducer(List<String> intermediate_location, String appClassPath, String outputFilePath) throws RemoteException;
}
