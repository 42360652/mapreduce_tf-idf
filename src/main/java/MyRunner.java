import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

public class MyRunner {

    private final static String FILE_IN_PATH = "src/data";
    private final static String FILE_OUT_PATH = "src/dataoutput";

    public static void main(String[] args) {
        MyRunner runner = new MyRunner();


        try {
            runner.execute();
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    private void execute() throws Exception {
        String tmpTFPath = FILE_OUT_PATH + "_tf";
        String tmpIDFPath = FILE_OUT_PATH + "_idf";
        String tmpTFIDFPath = FILE_OUT_PATH + "_tfidf";

        System.out.println("find words: ");
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        String findwords = bf.readLine();

        runTFJob(FILE_IN_PATH, tmpTFPath, findwords);
        //delete .crc file
        refreshFileList(tmpTFPath,".crc");
        runIDFJob(tmpTFPath, tmpIDFPath);
        refreshFileList(tmpIDFPath,".crc");
        runIntegrateJob(tmpTFPath, tmpIDFPath, tmpTFIDFPath);
        refreshFileList(tmpTFIDFPath,".crc");
        runTFIDFJob(tmpTFIDFPath, FILE_OUT_PATH);
    }

    private int runTFJob(String inputPath, String outputPath, String findwords) throws Exception {
        Configuration configuration = new Configuration();
        String regex = "\\b(" + findwords.replaceAll(" +","|") + ")\\b";
        configuration.set("mapregex",regex);

        File f = new File(outputPath);
        if (f.exists()){
            FileUtils.deleteDirectory(f);
        }

        Job job = Job.getInstance(configuration);
        job.setJobName("TF-job");
        job.setJarByClass(TFMapReduce.class);

        job.setMapperClass(TFMapReduce.TFMapper.class);
        job.setCombinerClass(TFMapReduce.TFCombiner.class);
        job.setPartitionerClass(TFMapReduce.TFPartitioner.class);
        job.setNumReduceTasks(getNumReduceTasks(configuration, inputPath));
        job.setReducerClass(TFMapReduce.TFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int runIDFJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();

        File f = new File(outputPath);
        if (f.exists()){
            FileUtils.deleteDirectory(f);
        }

        Job job = Job.getInstance(configuration);
        job.setJobName("IDF-job");
        job.setJarByClass(IDFMapReduce.class);

        job.setMapperClass(IDFMapReduce.IDFMapper.class);
        job.setReducerClass(IDFMapReduce.IDFReducer.class);
        job.setProfileParams(String.valueOf(getNumReduceTasks(configuration, inputPath)));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int runIntegrateJob(String inputTFPath, String inputIDFPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();

        File f = new File(outputPath);
        if (f.exists()){
            FileUtils.deleteDirectory(f);
        }

        Job job = Job.getInstance(configuration);
        job.setJobName("Integrate-job");
        job.setJarByClass(IntegrateMapReduce.class);

        job.setMapperClass(IntegrateMapReduce.IntegrateMapper.class);
        job.setReducerClass(IntegrateMapReduce.IntegrateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputTFPath));
        FileInputFormat.addInputPath(job, new Path(inputIDFPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int runTFIDFJob(String inputPath, String outputPath) throws Exception{
        Configuration configuration = new Configuration();

        File f = new File(outputPath);
        if (f.exists()){
            FileUtils.deleteDirectory(f);
        }

        Job job = Job.getInstance(configuration);
        job.setJobName("TFIDF-job");
        job.setJarByClass(TFIDFMapReduce.class);

        job.setMapperClass(TFIDFMapReduce.TFIDFMapper.class);
        job.setReducerClass(TFIDFMapReduce.TFIDFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    //the number of reduce tasks = the number of input files
    private int getNumReduceTasks(Configuration configuration, String inputPath) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        FileStatus status[] = fs.listStatus(new Path(inputPath));
        return status.length;

    }

    //delete .[type] file
    private static void refreshFileList(String strPath,String type) {
        File dir = new File(strPath);
        File[] files = dir.listFiles();
        if (files == null)
        {
            System.out.println("no file");
            return;
        }
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                refreshFileList(files[i].getAbsolutePath(),type);
            }else {
                String strFileName = files[i].getAbsolutePath().toLowerCase();
                if(strFileName.endsWith(type)){
                    System.out.println("deleting：" + strFileName);
                    files[i].delete();
                }
            }
        }
    }
}