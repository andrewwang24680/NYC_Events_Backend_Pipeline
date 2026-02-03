import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfilingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private Map<String, Integer> stats = new HashMap<>();
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        stats.put(key.toString(), sum);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sort the stats by key for consistent output
        Map<String, Integer> sortedStats = new TreeMap<>(stats);
        
        System.out.println("============================================================");
        System.out.println("DATA PROFILING RESULTS");
        System.out.println("============================================================");
        
        Integer total = stats.get("TOTAL_RECORDS");
        if (total != null && total > 0) {
            System.out.println(String.format("\nTotal Records: %,d", total));
            
            System.out.println("\n--- Field Validity Statistics ---");
            
            for (Map.Entry<String, Integer> entry : sortedStats.entrySet()) {
                String key = entry.getKey();
                Integer count = entry.getValue();
                
                if (!key.equals("TOTAL_RECORDS")) {
                    double percentage = (count * 100.0) / total;
                    System.out.println(String.format("%s: %,d (%.2f%%)", 
                        key, count, percentage));
                }
            }
            
            System.out.println("\n--- Data Quality Summary ---");
            int validCount = 0;
            int totalChecks = 0;
            
            for (Map.Entry<String, Integer> entry : stats.entrySet()) {
                String key = entry.getKey();
                Integer count = entry.getValue();
                
                if (!key.equals("TOTAL_RECORDS")) {
                    totalChecks += count;
                    if (key.contains("VALID")) {
                        validCount += count;
                    }
                }
            }
            
            double qualityScore = totalChecks > 0 ? (validCount * 100.0) / totalChecks : 0;
            System.out.println(String.format("Overall Data Quality Score: %.2f%%", qualityScore));
        }
        
        System.out.println("============================================================");
        
        System.out.println("\n--- Raw Statistics (TSV format) ---");
        for (Map.Entry<String, Integer> entry : sortedStats.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }
        
        // Also write to context output
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable();
        
        for (Map.Entry<String, Integer> entry : sortedStats.entrySet()) {
            outputKey.set(entry.getKey());
            outputValue.set(entry.getValue());
            context.write(outputKey, outputValue);
        }
    }
}