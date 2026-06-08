package it.unipi.cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChicagoCrimesApp {

    // ========================================================================
    // 1. CUSTOM WRITABLE: Encapsulates [arrest count, total count]
    // ========================================================================
    public static class ArrestMetricsWritable implements Writable {
        private long arrestCount;
        private long totalCount;

        public ArrestMetricsWritable() {}

        public ArrestMetricsWritable(long arrestCount, long totalCount) {
            this.arrestCount = arrestCount;
            this.totalCount = totalCount;
        }

        public void set(long arrestCount, long totalCount) {
            this.arrestCount = arrestCount;
            this.totalCount = totalCount;
        }

        public long getArrestCount() { return arrestCount; }
        public long getTotalCount() { return totalCount; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(arrestCount);
            out.writeLong(totalCount);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            arrestCount = in.readLong();
            totalCount = in.readLong();
        }

        @Override
        public String toString() {
            return arrestCount + "," + totalCount; // Output format: "15,40"
        }
    }

    // ========================================================================
    // JOB 1: GROUP CRIME TYPE AND LOCATION (BASELINE AGGREGATION)
    // ========================================================================
    public static class Job1Mapper extends Mapper<LongWritable, Text, Text, ArrestMetricsWritable> {
        private Text outKey = new Text();
        private ArrestMetricsWritable outValue = new ArrestMetricsWritable();
        private long malformedRecordsCount;

        @Override
        protected void setup(Context context) {
            malformedRecordsCount = 0;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header row
            if (key.get() == 0 && value.toString().contains("Primary Type")) return;

            // Smart CSV split regex (ignores commas inside quotes)
            String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            // The file has at least 22 columns; safely require more than 10 columns
            if (fields.length < 10) {
                malformedRecordsCount++;
                return;
            }

            // Updated to the correct column indexes for the real CSV file
            String primaryType = fields[6].trim().toUpperCase();      // Column 6: Primary Type
            String location = fields[8].trim().toUpperCase();         // Column 8: Location Description
            String arrestStr = fields[9].trim().toLowerCase();        // Column 9: Arrest (True/False)

            if (primaryType.isEmpty() || location.isEmpty()) {
                malformedRecordsCount++;
                return;
            }

            long isArrested = arrestStr.equals("true") ? 1 : 0;

            // Emitted composite key: "NARCOTICS|SIDEWALK"
            outKey.set(primaryType + "|" + location);
            outValue.set(isArrested, 1);
            context.write(outKey, outValue);
        }

        @Override
        protected void cleanup(Context context) {
            System.out.println("[AUDIT] Job 1 Mapper skipped " + malformedRecordsCount + " malformed rows.");
        }
    }

    public static class Job1Reducer extends Reducer<Text, ArrestMetricsWritable, Text, ArrestMetricsWritable> {
        private ArrestMetricsWritable result = new ArrestMetricsWritable();

        @Override
        protected void reduce(Text key, Iterable<ArrestMetricsWritable> values, Context context) throws IOException, InterruptedException {
            long sumArrests = 0;
            long sumTotal = 0;

            for (ArrestMetricsWritable val : values) {
                sumArrests += val.getArrestCount();
                sumTotal += val.getTotalCount();
            }

            result.set(sumArrests, sumTotal);
            context.write(key, result); // Output: "NARCOTICS|SIDEWALK \t 20,50"
        }
    }

    // ========================================================================
    // JOB 2: CALCULATE PERCENTAGES AND FIND THE LOCATION WITH THE LOWEST CLEARANCE RATE
    // ========================================================================
    public static class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) return;

            String[] keyParts = parts[0].split("\\|");
            if (keyParts.length != 2) return;

            String primaryType = keyParts[0];
            String location = keyParts[1];
            String metrics = parts[1]; // "20,50"

            outKey.set(primaryType);
            // Use "@@@" as a safe delimiter to avoid parsing issues
            outValue.set(location + "@@@" + metrics);
            context.write(outKey, outValue);
        }
    }

    // CUSTOM PARTITIONER: Send all records for the same crime type to one reducer
    public static class CrimeCategoryPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double worstRate = 101.0;
            String worstLocation = "";
            long totalArrestsForType = 0;
            long totalIncidentsForType = 0;

            for (Text val : values) {
                // Split using the safe delimiter
                String[] parts = val.toString().split("@@@");
                if(parts.length != 2) continue;

                String loc = parts[0];
                String[] counts = parts[1].split(",");

                try {
                    long arrests = Long.parseLong(counts[0]);
                    long totals = Long.parseLong(counts[1]);

                    // Filter noise: rank only locations with more than 10 incidents
                    if (totals > 10) {
                        double rate = ((double) arrests / totals) * 100.0;
                        if (rate < worstRate) {
                            worstRate = rate;
                            worstLocation = loc;
                        }
                    }

                    totalArrestsForType += arrests;
                    totalIncidentsForType += totals;
                } catch (NumberFormatException e) {
                    continue;
                }
            }

            // Write the final result
            if (!worstLocation.isEmpty()) {
                String report = String.format("Worst Location: %-25s (Arrest Rate: %5.2f%%) | Total Incidents of this type: %d",
                        worstLocation, worstRate, totalIncidentsForType);
                context.write(key, new Text(report));
            }
        }
    }

    // ========================================================================
    // CHAINED JOB CONFIGURATION
    // ========================================================================
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ChicagoCrimesApp <input_csv> <intermediate_dir> <final_output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // ------------------ Job 1 ------------------
        Job job1 = Job.getInstance(conf, "Job 1: Crime Baseline Aggregation");
        job1.setJarByClass(ChicagoCrimesApp.class);

        job1.setMapperClass(Job1Mapper.class);
        job1.setCombinerClass(Job1Reducer.class);
        job1.setReducerClass(Job1Reducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(ArrestMetricsWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(ArrestMetricsWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            System.err.println("Job 1 failed, exiting...");
            System.exit(1);
        }

        // ------------------ Job 2 ------------------
        Job job2 = Job.getInstance(conf, "Job 2: Spatial Hotspots Analytics");
        job2.setJarByClass(ChicagoCrimesApp.class);

        job2.setMapperClass(Job2Mapper.class);
        job2.setPartitionerClass(CrimeCategoryPartitioner.class);
        job2.setReducerClass(Job2Reducer.class);

        // Run multiple reducers in parallel
        job2.setNumReduceTasks(4);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
