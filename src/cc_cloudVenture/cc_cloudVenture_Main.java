package cc_cloudVenture;



	import java.io.DataOutputStream;
	import java.io.IOException;

	import org.apache.hadoop.fs.FSDataOutputStream;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapred.FileOutputFormat;
	import org.apache.hadoop.mapred.InputSplit;
	import org.apache.hadoop.mapred.JobClient;
	import org.apache.hadoop.mapred.JobConf;
	import org.apache.hadoop.mapred.RecordWriter;
	import org.apache.hadoop.mapred.Reporter;
	import org.apache.hadoop.mapred.TextOutputFormat;
	import org.apache.hadoop.record.CsvRecordOutput;
	import org.apache.hadoop.util.Progressable;

	import org.commoncrawl.hadoop.io.ARCInputFormat;
	import org.commoncrawl.hadoop.io.JetS3tARCSource;

	/**
	 * cc_cloudVenture_Main.java
	 * 
	 * This program helps analyze the server occurences in an .arc file. This program uses ArcFileReader library.
	 * We used tutorial example to help understand the features of this library written by Steve Salevan <steve.salevan@gmail.com>
	 * 
	 * @author Pankaj
	 * @author Amrut
	 * @author Pratik
	 * 
	 */
	public class cc_cloudVenture_Main {
	  /**
	   * Contains the Amazon S3 bucket holding the CommonCrawl corpus.
	   */
	  private static final String CC_BUCKET = "aws-publicdatasets";
	  
	  /**
	   * Outputs counted words into a CSV-formatted file specified by arguments in .jar
	   * Specified according to CommonCrawl corpus ArcFileReader library.
	   */
	  public static class CSVOutputFormat
	      extends TextOutputFormat<Text, LongWritable> {
	    public RecordWriter<Text, LongWritable> getRecordWriter(
	        FileSystem ignored, JobConf job, String name, Progressable progress)
	        throws IOException {
	      Path file = FileOutputFormat.getTaskOutputPath(job, name); //path for the results used by the distributed file system
	      FileSystem fs = file.getFileSystem(job);
	      FSDataOutputStream fileOut = fs.create(file, progress);
	      return new CSVRecordWriter(fileOut);
	    }
	/**
	 * Class CSVRecordWriter
	 * This class is used to record the output to CSV file.
	 * This is specified according to Common Crawl corpus ArcFileReader library
	 */
	    protected static class CSVRecordWriter
	        implements RecordWriter<Text, LongWritable> {
	      protected DataOutputStream outStream;

	      public CSVRecordWriter(DataOutputStream out) {
	        this.outStream = out;
	      }
	/**
	 * This is specified according to Common Crawl corpus ArcFileReader library
	 */
	      public synchronized void write(Text key, LongWritable value)
	          throws IOException {
	        CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);
	        csvOutput.writeString(key.toString(), "Server");
	        csvOutput.writeLong(value.get(), "occurrences");
	      }

	      public synchronized void close(Reporter reporter) throws IOException {
	        outStream.close();
	      }
	    }
	  }
	/**
	 * 
	 * @param args
	 * @throws IOException
	 * 
	 * The main method initiates the Operation on AWS EMR. 
	 */
	  public static void main(String[] args) throws IOException {
	    // Command line arguments for AWS Credentials, input and output sources.
		
	    String awsCredentials = args[0];
	    String awsSecretKey = args[1];
	    String inputFile = args[2];
	    String outputFile = args[3];

	  
	    // Creates a new job configuration for this Hadoop job.
	    JobConf configuration1 = new JobConf();
	    
	    // Configures this job with your Amazon AWS credentials using JetS3tArcSource library
	    configuration1.set(JetS3tARCSource.P_INPUT_PREFIXES, inputFile);
	    configuration1.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
	    configuration1.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecretKey);
	    configuration1.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);

	    // Configures where the input comes from when running our Hadoop job,
	    // in this case, gzipped ARC files from the specified Amazon S3 bucket
	    // paths.
	    ARCInputFormat.setARCSourceClass(configuration1, JetS3tARCSource.class);
	    ARCInputFormat inputFormat = new ARCInputFormat();
	    inputFormat.configure(configuration1);
	    configuration1.setInputFormat(ARCInputFormat.class);

	    // Configures what kind of Hadoop output we want.
	    configuration1.setOutputKeyClass(Text.class);
	    configuration1.setOutputValueClass(LongWritable.class);

	    // Configures where the output goes to when running our Hadoop job.
	    CSVOutputFormat.setOutputPath(configuration1, new Path(outputFile));
	    CSVOutputFormat.setCompressOutput(configuration1, false);
	    configuration1.setOutputFormat(CSVOutputFormat.class);

	    // Allows some (10%) of tasks fail; we might encounter the 
	    // occasional troublesome set of records and skipping a few 
	    // of 1000s won't hurt counts too much.
	    // Specified according to tutorial
	    configuration1.set("mapred.max.map.failures.percent", "10");

	    // Tells the user some context about this job.
	    InputSplit[] splits = inputFormat.getSplits(configuration1, 0);
	    if (splits.length == 0) {
	      System.out.println("ERROR: No .ARC files found!");
	      return;
	    }
	    System.out.println("Found " + splits.length + " InputSplits:");
	    for (InputSplit split : splits) {
	      System.out.println(" - will process file: " + split.toString());
	    }

	    // Tells Hadoop what Mapper and Reducer classes to use;
	    
	    configuration1.setMapperClass(cc_cloudVentureMapper.class);
	    configuration1.setCombinerClass(cc_cloudVentureReducer.class);
	    configuration1.setReducerClass(cc_cloudVentureReducer.class);

	    // Tells Hadoop mappers and reducers to pull dependent libraries from
	    // those bundled into this JAR.
	    configuration1.setJarByClass(cc_cloudVenture_Main.class);

	    // Runs the job.
	    JobClient.runJob(configuration1);
	  }
	}
