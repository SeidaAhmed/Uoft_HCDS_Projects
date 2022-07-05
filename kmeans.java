package mapreduce;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;


public class kmeans {

	public static  class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

         public final static String centroidfile="centers.txt";
         public float[][] centroids = new float[5][2];

         public void setup(Context context) throws IOException {
   	      Scanner reader = new Scanner(new FileReader(centroidfile));

   	   
   	      boolean exit = false;
   	    while (!exit) {
   	    for (int i = 0; i < 5; i++) {
   	        String valuesStrArr[] = reader.nextLine().split(",");
   	        for (int j = 0; j < 2; j++) {
   	            centroids[i][j] = Float.parseFloat(valuesStrArr[j]);
   	        }

   	        if (i == 5 - 1)
   	            exit = true;
   	    }
   	}
   	      
   	      
   	      
   	      
         }
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			  float distance=0;
			  float mindistance=999999999.9f;
			  int winnercentroid=-1;
			  int i=0;
			  for ( i=0; i<5; i++ ) {
				  
				  String val= value.toString();
				  List<String> valuelist = Arrays.asList(val.split(","));
				  float x= Float.parseFloat(valuelist.get(0));
				  float y= Float.parseFloat(valuelist.get(1));
			      distance = ( x-centroids[i][0])*(x-centroids[i][0]) + 
				  (y - centroids[i][1])*(y-centroids[i][1]);
			      if ( distance < mindistance ) {
				  mindistance = distance;
				  winnercentroid=i;
			      }

		}
			  
			  IntWritable winnerCentroids = new IntWritable(winnercentroid);
			  context.write(winnerCentroids, value);
			  


	}

	}
	
	public static class KmeansReducer 
    extends Reducer<IntWritable,Text,IntWritable,Text> {

		
		
		float[][] new_centroid=new float[5][2];

				@Override
				public void setup(Context context) {
				}
				

    public void reduce(IntWritable clusterid, Iterable<Text> points, 
			 Context context
	  ) throws IOException, InterruptedException {

	  int num = 0;
	  float centerx=0.0f;
	  float centery=0.0f;
	  
	  
	  for (Text point : points) {
		  String val= point.toString();
		  List<String> pointlist = Arrays.asList(val.split(","));
	      num++;
	      
	      float x = Float.parseFloat(pointlist.get(0));
	      float y = Float.parseFloat(pointlist.get(1));;
	      centerx += x;
	      centery += y;
	  }
	  centerx = centerx/num;
	  centery = centery/num;
	  
	  
	  String preres = String.format("%f, %f", centerx, centery,points);
	  Text result = new Text(preres);
	  context.write(clusterid, result);
    }
}
	
	
	
	
	public static void main(String[] args) throws Exception {
	    
		for (int i =0 ; i<20 ; i++) {
			
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: kmeans <in> <out>");
	      System.exit(2);
	    }

	    Job job = new Job(conf, "kmeans");
	    Path toCache = new Path("/user/Semir/centers/centers.txt");
	    job.addCacheFile(toCache.toUri());
	    job.createSymlink();
	    job.setJarByClass(kmeans.class);
	    job.setMapperClass(KmeansMapper.class);
	    job.setReducerClass(KmeansReducer.class);
	    job.setMapOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.setMapOutputKeyClass(IntWritable.class);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}
	



}