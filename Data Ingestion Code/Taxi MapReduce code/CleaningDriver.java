import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class CleaningDriver extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CleaningDriver(), args);
        System.exit(exitCode);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CleaningDriver <taxi_type> <input_path> <output_path>");
            System.err.println("taxi_type: yellow, green, or fhv");
            return -1;
        }
        
        String taxiType = args[0].toLowerCase();
        String inputPath = args[1];
        String outputPath = args[2];
        
        Configuration conf = getConf();
        
        conf.set("parquet.read.support.class", GroupReadSupport.class.getName());
        
        Job job = Job.getInstance(conf, "Taxi Data Cleaning (Parquet) - " + taxiType);
        
        job.setJarByClass(CleaningDriver.class);
        
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        
        switch (taxiType) {
            case "yellow":
                job.setMapperClass(CleaningYellowMapper.class);
                break;
            case "green":
                job.setMapperClass(CleaningGreenMapper.class);
                break;
            case "fhv":
                job.setMapperClass(CleaningFHVMapper.class);
                break;
            default:
                System.err.println("Invalid taxi type. Must be: yellow, green, or fhv");
                return -1;
        }
        
        job.setReducerClass(CleaningReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}