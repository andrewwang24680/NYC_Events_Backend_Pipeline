import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class ProfilingDriver extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ProfilingDriver(), args);
        System.exit(exitCode);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ProfilingDriver <taxi_type> <input_path> <output_path>");
            System.err.println("taxi_type: yellow, green, or fhv");
            return -1;
        }
        
        String taxiType = args[0].toLowerCase();
        String inputPath = args[1];
        String outputPath = args[2];
        
        Configuration conf = getConf();
        
        conf.set("parquet.read.support.class", GroupReadSupport.class.getName());
        
        Job job = Job.getInstance(conf, "Taxi Data Profiling (Parquet) - " + taxiType);
        
        job.setJarByClass(ProfilingDriver.class);
        
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        
        switch (taxiType) {
            case "yellow":
                job.setMapperClass(ProfilingYellowMapper.class);
                break;
            case "green":
                job.setMapperClass(ProfilingGreenMapper.class);
                break;
            case "fhv":
                job.setMapperClass(ProfilingFHVMapper.class);
                break;
            default:
                System.err.println("Invalid taxi type. Must be: yellow, green, or fhv");
                return -1;
        }
        
        job.setReducerClass(ProfilingReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}