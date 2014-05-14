import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class RelativeFrequencyMapper extends
		Mapper<LongWritable, Text, WordPair, IntWritable> {
	WordPair wp=new WordPair();
	IntWritable iw=new IntWritable();
	
	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		
		int neighbors=context.getConfiguration().getInt("neighbors", 2);
		String[] tokens=value.toString().split("\\s+");
		int size=tokens.length;
		for(int i=0;i<tokens.length;i++)
		{
			
			int start= ((i-neighbors)<0)?0:(i-neighbors);
			int end=((i+neighbors)>=size)?size-1:i+neighbors;
			wp.setFirst(new Text(tokens[i]));
			for(int j=start;j<=end;j++)
			{
				if(i==j)
					continue;
				wp.setSecond(new Text(tokens[j]));
				iw.set(1);
				
				context.write(wp, iw);
			}
			
			wp.setSecond(new Text("*"));
			iw.set(end-start);
			context.write(wp, iw);
			
		}
		
		
		
		
	}
	
	
	

}
