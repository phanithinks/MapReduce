import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordPairPartitiner extends Partitioner<WordPair, IntWritable> {

	@Override
	public int getPartition(WordPair arg0, IntWritable arg1, int arg2) {
		// TODO Auto-generated method stub
		
		return arg0.getFirst().hashCode()%arg2;
		
	}

}
