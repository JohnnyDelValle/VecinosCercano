//clase main

package hadoop1;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 

public class Hadoop1 { 
        
    public static void main(String args[])throws Exception 
   { 
      JobConf conf = new JobConf(conexion.class); 
      
      conf.setJobName("max_eletricityunits"); 
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class); 
      conf.setMapperClass(Mapp.class); 
      conf.setCombinerClass(Reduce.class); 
      conf.setReducerClass(Reduce.class); 
      conf.setInputFormat(TextInputFormat.class); 
      conf.setOutputFormat(TextOutputFormat.class); 
      
      FileInputFormat.setInputPaths(conf, new Path(args[0])); 
      FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
      
      JobClient.runJob(conf); 
   } 

}

//____________________________________________________________________

//clase map

package hadoop1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public  class Mapp extends MapReduceBase implements 
   Mapper<LongWritable ,/*Input key Type */ 
   Text,                /*Input value Type*/ 
   Text,                /*Output key Type*/ 
   IntWritable>        /*Output value Type*/ 
   { 
      
      //Map function 
      
      @Override
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,   
      Reporter reporter) throws IOException 
      { 
         String line = value.toString(); 
         String valor = null; 
         StringTokenizer s = new StringTokenizer(line,"\t"); 
         String marca = s.nextToken(); 
                  
         while(s.hasMoreTokens())
            {
               valor=s.nextToken();
            } 
            
         int precio = Integer.parseInt(valor); 
         output.collect( new Text(marca), new IntWritable(precio)); 
      }
   }      

//_______________________________________________________________________
//clase reduce

package hadoop1;

import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 

  public class Reduce extends MapReduceBase implements Reducer< Text, IntWritable, Text, IntWritable > 
   {  
   
      //Reduce function 
      public void reduce( Text key, Iterator <IntWritable> values, 
         OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
         { 
            int maxavg=30; 
            int val=Integer.MIN_VALUE; 
            
            while (values.hasNext()) 
            { 
               if((val=values.next().get())>maxavg) 
               { 
                  output.collect(key, new IntWritable(val)); 
               } 
            } 
 
         } 
   }  

