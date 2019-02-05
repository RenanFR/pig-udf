package pig.udf;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class LoadYoutubeData extends LoadFunc{
	
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	private RecordReader recordReader;
	
	public LoadYoutubeData() {
	}

	@Override//Defines the input format for the data set reading
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@Override//The logic for extracting the relevant fields and doing columns transformation on data set reading
	public Tuple getNext() throws IOException {
		try {  
			if (!recordReader.nextKeyValue()) {//If there's no more records to read
				return null;
			}
			Tuple tuple = tupleFactory.newTuple(4);
			String[] record = recordReader.getCurrentValue().toString().split(",");
			String rank = record[0];
			String channel = record[2];
			String subscribers = record[4];
			String views = record[5];
			tuple.set(0, new DataByteArray(rank));
			tuple.set(1, new DataByteArray(channel));
			tuple.set(2, new DataByteArray(subscribers));
			tuple.set(3, new DataByteArray(views));
			return tuple;
		} catch (InterruptedException interruptedException) {
			return null;
		}
	}

	@Override//Initialize auxiliary instances required to read the records
	public void prepareToRead(RecordReader recordReader, PigSplit pigSplit)
			throws IOException {
		this.recordReader = recordReader;
	}

	@Override//Set the input path for the data set 
	public void setLocation(String path, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, path);
	}

}
