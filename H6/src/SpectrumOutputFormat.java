import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class SpectrumOutputFormat extends FileOutputFormat<Text, DoubleWritable> {

  @Override
  public RecordWriter<Text, DoubleWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      // TODO
      return new SpectrumRecordWriter();
  }
}
