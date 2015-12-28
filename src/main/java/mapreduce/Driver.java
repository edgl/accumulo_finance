package mapreduce;

import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(exitCode);
	}
	
	static class Opts extends MapReduceClientOnRequiredTable {
		@Parameter(names = "--input", required = false)
		String input;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("");
		job.setJarByClass(Driver.class);
		job.setMapperClass(AddDateTextToStocks.class);
		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setNumReduceTasks(0);
		
		Opts opts = new Opts();
		opts.parseArgs(job.getJobName(), args, new Object[]{});
		opts.setAccumuloConfigs(job);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
