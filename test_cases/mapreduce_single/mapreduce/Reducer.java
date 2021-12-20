package mapreduce;

import java.util.List;
/*
 * This is a reducer interface that a user application should implement for reduce function
 */
public interface Reducer<S>{
	public List<S> reduce(String key, List values);
}