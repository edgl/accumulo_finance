package console;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;

import nasdaq.NasdaqListingFetcher;

public class LoadCompanyList {

	String zks       = "localhost:2181";
	String username  = "root";
	String tableName = "companies";
	String instanceName = "default";
	Instance instance = new ZooKeeperInstance(instanceName, zks);
	Connector connector; 
	private EXCHANGE exchange;
	
	public LoadCompanyList(EXCHANGE exchange) {
		this.exchange = exchange;
		try {
		connector = instance.getConnector(username, new PasswordToken(new byte[]{}));
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	public enum EXCHANGE {
		
		NASDAQ("http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download"),
		NYSE("http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download");
		
		private final String URL;
		
		EXCHANGE(String url) {
			this.URL = url;
		}
		
		String url() {
			return URL.toString();
		}
		
		String exchange() {
			return this.name().toLowerCase();
		}
	}
	
	public enum COMPANYLISTHEADERS {
		Symbol,
		Name,
		LastSale,
		MarketCap,
		IPOyear,
		Sector,
		Industry,
		Summary_Quote_URL
	}
	public static void main(String[] args) throws Exception {
		
		LoadCompanyList lcl = new LoadCompanyList(EXCHANGE.NYSE);
		lcl.load();
//		lcl.updateStats();
		
		lcl = new LoadCompanyList(EXCHANGE.NASDAQ);
		lcl.load();
//		lcl.updateStats();
		
		
	}
	private void updateStats() throws TableNotFoundException, MutationsRejectedException {
		Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
		BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());

		scanner.fetchColumn(new Text("attributes"), new Text("Sector"));
		for(Entry<Key,Value> entry: scanner) {
			Mutation m = new Mutation(entry.getKey().getRow());
			m.put("Sector", entry.getValue().toString(), new Value());
			m.put("Exchange", exchange.exchange(), new Value());
			bw.addMutation(m);
		}
		
		scanner.clearColumns();
		scanner.fetchColumn(new Text("attributes"), new Text("Industry"));
		for(Entry<Key,Value> entry: scanner) {
			Mutation m = new Mutation(entry.getKey().getRow());
			m.put("Industry", entry.getValue().toString(), new Value());
			bw.addMutation(m);
		}
		
		bw.close();
		scanner.close();
		
	}
	
	private void load() throws TableNotFoundException, MutationsRejectedException, IOException {
		BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
		NasdaqListingFetcher fetcher = new NasdaqListingFetcher();
		fetcher.setHeaders(Arrays.asList(
				COMPANYLISTHEADERS.Symbol.toString(),
				COMPANYLISTHEADERS.Name.toString(),
				COMPANYLISTHEADERS.LastSale.toString(),
				COMPANYLISTHEADERS.MarketCap.toString(),
				COMPANYLISTHEADERS.IPOyear.toString(),
				COMPANYLISTHEADERS.Sector.toString(),
				COMPANYLISTHEADERS.Industry.toString(),
				COMPANYLISTHEADERS.Summary_Quote_URL.toString()));
				
		for(CSVRecord record : fetcher.getIterator(exchange.url(), ',')) {
			Mutation m = new Mutation(record.get(COMPANYLISTHEADERS.Symbol).trim());
			for(COMPANYLISTHEADERS header : COMPANYLISTHEADERS.values()) {
				m.put(header.toString(), record.get(header), new Value());
			}
			m.put("Exchange", exchange.exchange(), new Value());
			bw.addMutation(m);
			
		}
		
		bw.close();
		
	}
}
