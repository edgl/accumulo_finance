package mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.primitives.Longs;

public class AddDateTextToStocks extends Mapper<Key, Value, Text, Mutation> {
	
	private long currentTimestamp = 0L; 
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	protected void map(Key key, Value value, Mapper<Key, Value, Text, Mutation>.Context context)
			throws IOException, InterruptedException {
		
		long ts = Longs.fromByteArray(key.getColumnFamilyData().toArray());
		if(currentTimestamp != ts) {
			currentTimestamp = ts;
			context.write(new Text("stocks"), createMutation(key, value));
		}		
		
	}

	private Mutation createMutation(Key key, Value value) {
		// TODO Auto-generated method stub
		Mutation m = new Mutation(key.getRow());
		Date d = new Date(currentTimestamp);
		long time = d.getTime();
		
		m.put(key.getColumnFamily(), new Text("DateAsText"), new Value(sdf.format(d).getBytes()));
		m.put(key.getColumnFamily(), new Text("TimeAsLong"), new Value(Longs.toByteArray(time)));
				
		return m;
	}

}
