import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RelativeFrequencyDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Job job=Job.getInstance(this.getConf(),"Relative Frequency Pairs");
		
		job.setJarByClass(RelativeFrequencyDriver.class);
		job.setMapperClass(RelativeFrequencyMapper.class);
		job.setReducerClass(RelativeFrequencyReducer.class);
		job.setCombinerClass(RelativeFrequencyCombiner.class);
		job.setPartitionerClass(WordPairPartitiner.class);
		
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		
		
		
		return job.waitForCompletion(true)?0:1;
		
	}

	
	
	
	public static void main(String args[])
	{
		if(args.length<=0)
		{
			System.out.println("The pass input and output paths");
			System.exit(-1);
		}
		
		try {
			ToolRunner.run(new RelativeFrequencyDriver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	
	
	
}
