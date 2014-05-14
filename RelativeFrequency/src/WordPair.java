

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements Writable,WritableComparable<WordPair>{

	private Text first;
	private Text second;
	
	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	
	public WordPair()
	{
		set(new Text(),new Text());
	}
	
	public void setFirst(Text first) {
		this.first = first;
	}

	public void setSecond(Text second) {
		this.second = second;
	}

	public WordPair(String first,String second)
	{
		set(new Text(first),new Text(second));
		
	}
	
	private void set(Text first,Text second)
	{
		this.first=first;
		this.second=second;
	}
	
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
		
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
		first.write(arg0);
		second.write(arg0);
		
	}

	@Override
	public int compareTo(WordPair o) {
		// TODO Auto-generated method stub
		int cmp=first.compareTo(o.first);
		if(cmp!=0)
			return cmp;
		
		return second.compareTo(o.second);
	}

	
	public int hashCode()
	{
		int result = first != null ? first.hashCode() : 0;
        result = 163 * result + (second != null ? second.hashCode() : 0);
        return result;
	}

	public String toString()
	{
		return first.toString()+"\t"+second.toString();
	}
	
	public boolean equals(Object o){
		
		if(o instanceof WordPair)
		{
			WordPair wp=(WordPair)o;
			return first.equals(wp.first) && second.equals(wp.second);
		}
		return false;
		
		
	}

}
