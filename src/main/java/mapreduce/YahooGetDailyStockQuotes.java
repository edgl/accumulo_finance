package mapreduce;

import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

public class YahooGetDailyStockQuotes extends Configured implements Tool {

	public static final Opts opts = new Opts();
	public static Connector accumuloConnector;
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new YahooGetDailyStockQuotes(), args);
		System.exit(exitCode);
	}
	
	static class Opts extends MapReduceClientOnRequiredTable {
		@Parameter(names = "--input", required = true)
		String input;
	}
	
	@Override
	public int run(String[] args) throws Exception {
				
		
		// setup the job
		Configuration CONF = getConf();
		Job job = Job.getInstance(CONF);
		job.setJobName("Process ALl Stocks");
		job.setJarByClass(ProcessStockFiles.class);
		job.setMapperClass(YahooGetDailStockQuotesMapper.class);
		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setNumReduceTasks(0);
		
		opts.parseArgs(job.getJobName(), args, new Object[]{});
		opts.setAccumuloConfigs(job);
		
		
		
		NLineInputFormat.addInputPath(job, new Path(opts.input));
		NLineInputFormat.setNumLinesPerSplit(job, 10);
		
		job.getConfiguration().set("tablename", opts.getTableName());
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
