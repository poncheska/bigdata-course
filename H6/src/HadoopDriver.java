import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class HadoopDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
    System.exit(ret);
  }

  public int run(String[] args) throws Exception {

    if (args.length < 2) {
      ToolRunner.printGenericCommandUsage(System.err);
      System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
      System.exit(1);
    }

    boolean countTotal = false;
    if (args.length >= 3 && "--total".equals(args[3])) {
      countTotal = true;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(HadoopDriver.class);
    job.setJobName("Spectrum");
    System.out.println("Input dirs: " + new Path(args[0]));
    //FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(SpectrumMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setInputFormatClass(SpectrumInputFormat.class);

    System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
    System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

    long t0 = System.currentTimeMillis();
    int ret = job.waitForCompletion(true) ? 0 : 1;
    long t1 = System.currentTimeMillis();
    double dt = (t1-t0)*1e-3;
    System.out.println("Time: " + dt + "s");
    return ret;
  }
}
