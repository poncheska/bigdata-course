import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class SpectrumMapper extends Mapper<Text, RawSpectrum, Text, DoubleWritable> {

  public void map(Text key, RawSpectrum value, Context context)
      throws IOException, InterruptedException {
      context.write(key, new DoubleWritable(value.variance()));
  }

}
