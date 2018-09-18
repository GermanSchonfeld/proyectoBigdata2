package com.master.bigdata;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * AccidentMapReduce Class
 *
 */
public class AccidentMapReduce extends Configured implements Tool {


    public static class AccidentMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final String SEPARATOR = ";";

        /** El formato que tenemos es el siguiente:
         Date;TimeRange; WeekDay;District;Place; PlaceNumber;PartID;Hail;Ice; Rain;Fog;Dry;Snow;Wet;Oil;Mud;Gravel;
         IceRoad;DryAndClean;Victims;VehicleType;PersonType; Sex;AgeRange;
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String[] values = value.toString().split(SEPARATOR);


            final String hail = format(values[7]);
            final String ice = format(values[8]);
            final String rain = format(values[9]);
            final String fog = format(values[10]);
            final String dry = format(values[11]);
            final String snow = format(values[12]);
            final String wet = format(values[13]);
            final String oil = format(values[14]);
            final String mud = format(values[15]);
            final String gravel = format(values[16]);
            final String iceRoad = format(values[17]);
            final String dryAndClean = format(values[18]);
            final String victims = format(values[19]);




            if (NumberUtils.isNumber(victims.toString())) {
                int intValue = NumberUtils.toInt(victims);

                if (isTrue(hail)) {
                    context.write(new Text("hail"), new IntWritable(intValue));
                }
                if (isTrue(ice)) {
                    context.write(new Text("ice"), new IntWritable(intValue));
                }
                if (isTrue(rain)) {
                    context.write(new Text("rain"), new IntWritable(intValue));
                }
                if (isTrue(fog)) {
                    context.write(new Text("fog"), new IntWritable(intValue));
                }
                if (isTrue(dry)) {
                    context.write(new Text("dry"), new IntWritable(intValue));
                }
                if (isTrue(snow)) {
                    context.write(new Text("snow"), new IntWritable(intValue));
                }
                if (isTrue(wet)) {
                    context.write(new Text("wet"), new IntWritable(intValue));
                }
                if (isTrue(oil)) {
                    context.write(new Text("oil"), new IntWritable(intValue));
                }
                if (isTrue(mud)) {
                    context.write(new Text("mud"), new IntWritable(intValue));
                }
                if (isTrue(gravel)) {
                    context.write(new Text("gravel"), new IntWritable(intValue));
                }
                if (isTrue(iceRoad)) {
                    context.write(new Text("iceRoad"), new IntWritable(intValue));
                }
                if (isTrue(dryAndClean)) {
                    context.write(new Text("dryAndClean"), new IntWritable(intValue));
                }

            }
        }

        private String format(String value) {
            return value.trim();
        }

        private boolean isTrue(String value) {
            return value.compareTo("SI")==0;
        }


    }


    public static class AccidentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {



        public void reduce(Text key, Iterable<IntWritable> victimsValues, Context context) throws IOException, InterruptedException {
            int totalVictims=0;

            for (IntWritable victims : victimsValues) {
                totalVictims += victims.get();

            }


            context.write(key, new IntWritable(totalVictims));

        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("AccidentMapReduce required params: {input file} {output dir}");
            System.exit(-1);
        }

        deleteOutputFileIfExists(args);

        final Job job = new Job(getConf());
        job.setJarByClass(AccidentMapReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(AccidentMapper.class);
        job.setReducerClass(AccidentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }


    private void deleteOutputFileIfExists(String[] args) throws IOException {
        final Path output = new Path(args[1]);
        FileSystem.get(output.toUri(), getConf()).delete(output, true);
    }



    public static void main(String[] args) throws Exception {
    ToolRunner.run(new AccidentMapReduce(), args);
    }
}
