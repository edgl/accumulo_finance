package nasdaq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

public class NasdaqListingFetcher {
	public static String NASDAQ_LISTED_URL = "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt";
	private char delimitter = ',';
	private List<String> headers = new ArrayList<String>();
	
	private static final Logger logger = Logger.getLogger(NasdaqListingFetcher.class);
	
	public Iterable<CSVRecord> getIterator(String URL, char delimitter) throws IOException {
		
		BufferedReader br = new BufferedReader(getReaderFromURL(URL));
		CSVFormat format = CSVFormat.DEFAULT.withDelimiter(delimitter);
		if(headers != null && headers.size() > 0)
			format = format.withHeader(headers.toArray(new String[]{}));
			
        CSVParser parser = new CSVParser(br, format);
        
        
		return parser;
	}
	
	public Iterable<CSVRecord> getIterator(String URL) throws IOException {
		return getIterator(URL, delimitter);
	}
	
	
	public List<String> getNasdaqStockSymbols() throws IOException {
        List<String> result = new ArrayList<String>();
        for(CSVRecord record: getIterator(NASDAQ_LISTED_URL, '|')) {
        	result.add(record.get(0));
        }

        return result;
    }
	
	public Reader getReaderFromURL(String URLPath) throws IOException {
		
		NasdaqListingFetcher.logger.info("Sending request: " + URLPath);
		
		URL request = new URL(URLPath);
        URLConnection connection = request.openConnection();
        InputStreamReader is = new InputStreamReader(connection.getInputStream());
               
        return is;
	}
	
	public Reader getReaderFromURL() throws IOException {
		return getReaderFromURL(NASDAQ_LISTED_URL);
	}

	public void setHeaders(List<String> headers) {
		this.headers = headers;
		
	}
	

}
