import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RelativeFrequencyReducer extends
		Reducer<WordPair, IntWritable, WordPair, Text> {

	Text currentWord=new Text("------");
	DoubleWritable totalCount=new DoubleWritable();
	Text falg=new Text("*");
	
	
	
	
	
	
	protected void reduce(WordPair key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		if(key.getSecond().equals(falg))
		{
			if(key.getFirst().equals(currentWord))
			{
				totalCount.set(totalCount.get()+getTotalValues(values));
			}else
			{
				currentWord.set(key.getSecond());
				
				totalCount.set(0);
				totalCount.set(getTotalValues(values));
			}
		}
		else{
			int count = getTotalValues(values);
			double relativeFre= (double) count/totalCount.get();
			context.write(key, new Text("Total:"+count+"    Relative Frequencey:"+relativeFre));
		}
			
		
		
		
		
		
		
	}
	
	
	private int  getTotalValues(Iterable<IntWritable> values)
	{
		int total=0;
		
		for(IntWritable value:values )
		{
			total+=value.get();
		}
		
		return total;
	}
	
	
	
	
	
	
	
}
