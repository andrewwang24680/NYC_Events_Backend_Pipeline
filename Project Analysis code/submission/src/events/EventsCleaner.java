package events;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EventsCleaner extends Configured implements Tool {
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: EventsCleaner <input_path> <output_path> <taxi_zones_path>");
            return -1;
        }
        
        Configuration conf = getConf();
        
        String inputPath = args[0];
        String outputPath = args[1];
        String taxiZonesPath = args[2];

        Job job = Job.getInstance(conf, "Events Cleaner");

        job.addCacheFile(new Path(taxiZonesPath).toUri());

        job.setJarByClass(EventsCleaner.class);
        job.setMapperClass(EventsCleanMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        Path outputDir = new Path(outputPath);
        outputDir.getFileSystem(conf).delete(outputDir, true);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new EventsCleaner(), args);
        System.exit(exitCode);
    }
}
