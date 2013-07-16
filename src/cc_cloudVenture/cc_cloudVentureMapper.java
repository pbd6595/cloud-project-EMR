package cc_cloudVenture;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.commoncrawl.protocol.shared.ArcFileItem;

import org.jsoup.Jsoup;

/**
 * Outputs differ for different Map functions.
 * @author Pankaj
 * @author Pratik
 * @author Amrut
 * 
 * Specified according to tutorial by Steve Salevan <steve.salevan@gmail.com>
 */
public class cc_cloudVentureMapper extends MapReduceBase 
  implements Mapper<Text, ArcFileItem, Text, LongWritable> {

	
  public void map(Text key, ArcFileItem arcfile1,
      OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException {
          // Retrieves page content from the passed-in ArcFileItem.
      ByteArrayInputStream inputStream = new ByteArrayInputStream(
          arcfile1.getContent().getReadOnlyBytes(), 0,
          arcfile1.getContent().getCount());
      // Converts InputStream to a String.
      String content = new Scanner(inputStream).useDelimiter("\\A").next();
      // Parses HTML with a tolerant parser and extracts all text.
      String pageText = Jsoup.parse(content).text();
      // Removes all punctuation.
	  pageText = pageText.replaceAll("[^a-zA-Z0-9\\<\\>\\.\\$\\#\\;\\:\\?\\(\\)\\@\\!\\+\\=\\/\\*]", "");
      // Normalizes whitespace to single spaces.
      pageText = pageText.replaceAll("\\s+", " ");
      // Splits by space and outputs to OutputCollector.
	  //The regular expression for mimeType
      //extracts content type in meta tag in html page
      String mimeTypePattern="<meta\\s*(?i)\\s*=\\s*(\"([^\"]*\")|'[^']*'content-type=([^'\">\\s]+))>";
      
      //The regular expression for extracting Server information
      String serverPattern="(?i)Server\\s*=\\s*(\"([^\"]*\")|'[^']*'";
      
      //The regular expression for extracting dynamic pattern from web page
      String dynamic_Pattern="<(?i)\\A\\?php|script|asp\\s*(?i)\\s*=\\s*(\"([^\"]*\")|'[^']*>";
      
      //Extraction works one at a time as output specifications are different
     //extract mimeType 
	  for (String word: pageText.split(" ")) {
    	 if(word.matches(mimeTypePattern))
    	 {
    	  output.collect(new Text(word), new LongWritable(1));  
    	 }
    	 
    
  }
	  //extract server name
	  for (String word: pageText.split(" ")) {
	    	 if(word.matches(serverPattern))
	    	 {
	    	  output.collect(new Text(word), new LongWritable(1));  
	    	 }
	    	 
	    
	  }
	  /*
	  //extract dynamic content
	  for (String word: pageText.split(" ")) {
		  String url=value.getUri();
		  
	    	 if(word.matches(mimeTypePattern))
	    	 {
	    	  output.collect(new Text(url), new Text(dynamic_Pattern));  
	    	 }
	    	 
	    
	  }*/
	  }
}