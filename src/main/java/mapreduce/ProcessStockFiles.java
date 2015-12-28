package mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;

import accumulo.LoadToAccumulo;
import accumulo.LoadToAccumulo.SymbolLine;


public class ProcessStockFiles extends Configured implements Tool {
	
	static Configuration CONF = null;
	static final String STOCK_INPUT_DIR = "STOCKS/";
	public static final Logger LOG = Logger.getLogger(ProcessStockFiles.class);
	
	public static void main(String[] args) throws Exception  {
		int exitCode = ToolRunner.run(new Configuration(), new ProcessStockFiles(), args);
		System.exit(exitCode);
	}
	
	/* MAPPER */
	static class ProcessStocksMapper extends Mapper<LongWritable, Text, Text, Mutation> {
		LoadToAccumulo load = new LoadToAccumulo();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Mutation>.Context context)
				throws IOException, InterruptedException {
			
			// Value indicates a file
			// get the file
			String fileName = value.toString();
			if(Strings.isNullOrEmpty(fileName))
				return;
			
			System.err.println(fileName);
			String stockName = fileName.split("\\.")[0];
			FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"), context.getConfiguration());
			Path p = new Path("hdfs://localhost:9000/user/egleeck/" + STOCK_INPUT_DIR + value.toString());
			load.setFileInputStream(fs.open(p));
			
			List<SymbolLine> sls = load.getAllSymbolLinesFromFile(stockName);
			System.out.println("Sybols count: " + sls.size());
			
			for (SymbolLine sl : sls) 
				context.write(new Text("stocks"), load.createMutation(
						sl.getSymbol(), sl.getDate(), sl.getAttibute(), sl.getValue()));
			
		}
	}

	static class Opts extends MapReduceClientOnRequiredTable {
		@Parameter(names = "--input", required = true)
		String input;
	}
	@Override
	public int run(String[] args) throws Exception {
				
		
		// setup the job
		CONF = getConf();
		Job job = Job.getInstance(CONF);
		job.setJobName("Process ALl Stocks");
		job.setJarByClass(ProcessStockFiles.class);
		job.setMapperClass(ProcessStocksMapper.class);
		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setNumReduceTasks(0);
		
		Opts opts = new Opts();
		opts.parseArgs(job.getJobName(), args, new Object[]{});
		opts.setAccumuloConfigs(job);
		
		
		NLineInputFormat.addInputPath(job, new Path(opts.input));
		NLineInputFormat.setNumLinesPerSplit(job, 10);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
