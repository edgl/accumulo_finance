package accumulo;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.googlecode.jcsv.CSVStrategy;
import com.googlecode.jcsv.reader.CSVReader;
import com.googlecode.jcsv.reader.internal.CSVReaderBuilder;
import com.googlecode.jcsv.reader.internal.DefaultCSVEntryParser;

public class LoadToAccumulo {

	private static final SimpleDateFormat frm = new SimpleDateFormat("yyyy-MM-dd");
	private BatchWriter batchWriter;
	private InputStream fileInputStream;
	private final ColumnVisibility vis = new ColumnVisibility();
	
	public void setFileInputStream(InputStream fileInputStream) {
		this.fileInputStream = fileInputStream;
	}
	
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
		ClientOnRequiredTable opts = new ClientOnRequiredTable();
		
		opts.parseArgs("Accumulo Load Symbol", args, new Object[]{});
		
		BatchWriterOpts bopts = new BatchWriterOpts();
		LoadToAccumulo load = new LoadToAccumulo();
		Connector con = opts.getConnector();
		
		load.batchWriter = con.createBatchWriter(opts.getTableName(), bopts.getBatchWriterConfig());
		
		for (SymbolLine sl : load.getAllSymbolLinesFromFile()) 
			load.batchWriter.addMutation(load.createMutation(sl.getSymbol(), sl.getDate(), sl.getAttibute(), sl.getValue()));
		
		load.batchWriter.flush();
		
		
	}
	

	public List<SymbolLine> getAllSymbolLinesFromFile() throws IOException {
		return getAllSymbolLinesFromFile("SPY");
			
	}
	
	public List<SymbolLine> getAllSymbolLinesFromFile(String symbol) throws IOException {
		Preconditions.checkNotNull(fileInputStream);
		
		List<SymbolLine> sl = new ArrayList<SymbolLine>(5000);
		
		Reader reader = new InputStreamReader(fileInputStream);
		System.out.println("fileINputStream: " + fileInputStream);
		
		CSVReaderBuilder<String[]> builder = new CSVReaderBuilder<>(reader);
		builder.strategy(new CSVStrategy(' ', '"', '#', false, true));
		
		CSVReader<String[]> csvParser = builder.entryParser(new DefaultCSVEntryParser()).build();
				
		
		List<String> headers = csvParser.readHeader();
		
		System.out.println("Header count: " + headers.size());
		
		List<String[]> data = csvParser.readAll();
		
		System.out.println("data count: " + data.size());
		
		for(String[] fields : data) {
			for(int headerIdx = 1; headerIdx < headers.size(); headerIdx++) {
				sl.add(new SymbolLine(symbol, fields[0], headers.get(headerIdx), fields[headerIdx]));
			}
		}
		
		return sl;
	}

	public static class SymbolLine {
		String symbol;
		String date;
		String attibute;
		String value;
		
		public SymbolLine(String symbol, String date, String attibute, String value) {
			super();
			this.symbol = symbol;
			this.date = date;
			
			if(attibute.contains("."))
				this.attibute = attibute.split("\\.")[1];
			else
				this.attibute = attibute;
		
			this.value = value;
		}
		public String getSymbol() {
			return symbol;
		}
		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}
		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}
		public String getAttibute() {
			return attibute;
		}
		public void setAttibute(String attibute) {
			this.attibute = attibute;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		
		
	}
	
	public Mutation createMutation(String symbol, String date, String attribute, String value) {
		
		Mutation mutation = new Mutation(symbol.getBytes());
		
		try {
			mutation.put(Longs.toByteArray(frm.parse(date).getTime()), 
					attribute.getBytes(), 
					this.vis, 
					value.getBytes());
		} catch (ParseException e) {
			System.err.println("Unable to parse date: " + e.getMessage());
		}
		
		
		return mutation;
	}
}
