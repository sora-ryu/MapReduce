package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface MapperService extends Remote {
	
	/*
	 * 	this is the interface of remote object  MapperService 
	 */
	public String masterLaundchMapper(String appClassPath, String filePath)throws RemoteException;
}
