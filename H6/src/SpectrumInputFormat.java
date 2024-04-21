import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.*;

public class SpectrumInputFormat extends InputFormat<Text, RawSpectrum> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
       Map<String, List<FileStatus>> stationData = readStationData(context);
       return generateSplits(stationData, context);
  }

  @Override
  public RecordReader<Text, RawSpectrum> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
       SpectrumRecordReader reader =  new SpectrumRecordReader();
       reader.initialize(split, context);
       return reader;
  }

  private Map<String, List<FileStatus>> readStationData(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        Path inp = FileInputFormat.getInputPaths(context)[0];
        FileSystem fs = FileSystem.get(conf);

        List<FileStatus> files = Arrays.asList(fs.listStatus(inp));
        Map<String, List<FileStatus>> stations = new HashMap<String, List<FileStatus>>();

        for (FileStatus file: files) {
          Path path = file.getPath();
          String filename = path.getName().toString();
          String station = filename.substring(0, 5);
          String variable = filename.substring(5, 6);
          String year = filename.substring(6, 10);
	
          List<FileStatus> stationFiles = stations.getOrDefault(station, new ArrayList<FileStatus>());
          stationFiles.add(file);
          stations.put(station, stationFiles);
        }

        return stations;
  }

  private List<InputSplit> generateSplits(Map<String, List<FileStatus>> stations, JobContext context)
      throws IOException {
      List<InputSplit> res = new ArrayList<InputSplit>();
      for (String station : stations.keySet()) {
         List<FileStatus> filesList = stations.get(station); 
           Path[] filesPath = new Path[filesList.size()] ;
           long[] lengths = new long[filesList.size()];

           for (int i = 0; i < filesList.size(); i++) {
                 filesPath[i] = filesList.get(i).getPath();
                 lengths[i] = filesList.get(i).getLen();
           }
          
           res.add(new CombineFileSplit(filesPath, lengths));
      }
      return res;
  }

}


