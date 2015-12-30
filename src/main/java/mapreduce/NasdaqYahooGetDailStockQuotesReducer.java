package mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import accumulo.LoadToAccumulo;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

public class NasdaqYahooGetDailStockQuotesReducer extends Reducer<IntWritable, Text, Text, Mutation> {

	/* Initialize the calendars */
	private static final Calendar to = Calendar.getInstance();
	private static final Calendar fromOneMonth = Calendar.getInstance();
	private static final Calendar fromTwentyYears = Calendar.getInstance();
	static {
		fromOneMonth.add(Calendar.MONTH, -1);
		fromTwentyYears.add(Calendar.YEAR, -20);
	}
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static Text TABLE_NAME;
	private static final LoadToAccumulo load = new LoadToAccumulo();
	private static final Instance inst = new ZooKeeperInstance("default", "localhost:2181");			
	private static Connector accumuloConnector;
	
	private static final Logger logger = Logger.getLogger(NasdaqYahooGetDailStockQuotesReducer.class);
	
	private Scanner scanner;
	
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, Text, Mutation>.Context context) throws IOException, InterruptedException {
		
		for(Text val : values) {
			String symbol = val.toString();
			Calendar from;
			if(symbolExists(symbol))
				from = fromOneMonth;
			else
				from = fromTwentyYears;
	
			Stock stock = null;
			try {
				stock = YahooFinance.get(symbol, from, to, Interval.DAILY);
			}
			catch(IOException ex) {
				logger.error(String.format("%s: Unable to get quotes for dates [%2$tY-%2$tm-%2$td] - [%3$tY-%3$tm-%3$td]",
						symbol, from, to));
				return;
			}
			List<HistoricalQuote> history = stock.getHistory();
			
			for(HistoricalQuote hq: history) {
				long dateAsLong = hq.getDate().getTimeInMillis();
				String dateAsLongString  = Long.toString(dateAsLong);
				
				Mutation m = new Mutation(symbol);
				m.put(dateAsLongString, "Volume", Long.toString(hq.getVolume()));
				m.put(dateAsLongString, "Close", hq.getClose().toString());
				m.put(dateAsLongString, "Adjusted", hq.getAdjClose().toString());
				m.put(dateAsLongString, "Open", hq.getOpen().toString());
				m.put(dateAsLongString, "High", hq.getHigh().toString());
				m.put(dateAsLongString, "Low", hq.getLow().toString());
				m.put(dateAsLongString, "DateAsText", sdf.format(new Date(dateAsLong)));
				context.write(TABLE_NAME, m);
			}
		}
	}
	
	@Override
	protected void setup(Reducer<IntWritable, Text, Text, Mutation>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		TABLE_NAME = new Text(context.getConfiguration().get("tablename"));
		try {
			accumuloConnector = inst.getConnector("root", new PasswordToken());
		} catch (AccumuloException | AccumuloSecurityException e) {
			throw new RuntimeException(e);
		}
	}
	
	private boolean symbolExists(String symbol) {
		if(scanner == null) {
			try {
				scanner = accumuloConnector.createScanner(TABLE_NAME.toString(), Authorizations.EMPTY);
			} catch (TableNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		else {
			scanner.clearColumns();
		}
		
		scanner.setRange(Range.exact(symbol));
		scanner.setBatchSize(1);
		boolean exists = false;
		for(Entry<Key, Value> entry: scanner) {
			// if it exists, then there's an entry
			// just get the 1 month prior information
			exists = true;
			break;
		}
		
		return exists;
	}
}
