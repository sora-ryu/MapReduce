package mapreduce;

import java.util.List;
import java.util.Map;

public interface Mapper<T,S> {
	public List<Map<T, S>> map(Object key, String value);
}
