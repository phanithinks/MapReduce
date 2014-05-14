import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class RelativeFrequencyCombiner extends
		Reducer<WordPair, IntWritable, WordPair, IntWritable> {

	
	
	protected void reduce(WordPair key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		
		int total=0;
		for(IntWritable value:values)
		{
			total+=value.get();
		}
		
		context.write(key, new IntWritable(total));
	}
	
	
	
}
