import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class TFMapReduce {
    /*
     * map input: file
     *     output: !:filename, the number of words
     *             word:filename, 1
     *
     * */
    static class TFMapper extends Mapper<Object, Text, Text, Text> {
        private final Text one = new Text("1");
        private Text label = new Text();
        private int allWordCount = 0;
        private String fileName = "";
        private String mapRegex=null;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) {
            fileName = getInputSplitFileName(context.getInputSplit());
            Configuration conf = context.getConfiguration();
            mapRegex = conf.get("mapregex");
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                allWordCount++;
                String word = tokenizer.nextToken();
                if (word.matches(mapRegex)){
                    label.set(String.join(":", word, fileName));
                    context.write(label, one);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(new Text("!:" + fileName), new Text(String.valueOf(allWordCount)));
        }

        private String getInputSplitFileName(InputSplit inputSplit) {
            String fileFullName = ((FileSplit)inputSplit).getPath().toString();
            String[] nameSegments = fileFullName.split("/");
            return nameSegments[nameSegments.length - 1];
        }


    }
    /*
    * combiner: input: !:filename, the number of words
     *                 word:filename, 1
     *          output: !:filename, the number of words
     *                 word:filename, the number of this word in this block
    * */
    static class TFCombiner extends Reducer<Text, Text, Text, Text> {
        private int allWordCount = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            if (values == null) {
                return;
            }

            if(key.toString().startsWith("!")) {
                allWordCount = Integer.parseInt(values.iterator().next().toString());
                context.write(key, new Text(String.valueOf(allWordCount)));
                return;
            }

            int sumCount = 0;
            for (Text value : values) {
                sumCount += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(String.valueOf(sumCount)));
        }
    }

    /*
    * reduce: input: !:filename, the number of words
     *               word:filename, the number of this word in a block
     *        output: tf
    * */
    static class TFReducer extends Reducer<Text, Text, Text, Text> {
        int allWordCount = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }

            if (key.toString().startsWith("!")) {
                for (Text value : values) {
                    allWordCount += Integer.parseInt(value.toString());
                    return;
                }
            }

            int sumCount = 0;
            for (Text value : values) {
                sumCount += Integer.parseInt(value.toString());
            }

            double tf = 1.0 * sumCount / allWordCount;
            context.write(key, new Text(String.valueOf(tf)));
        }
    }

    static class TFPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String fileName = key.toString().split(":")[1];
            return Math.abs((fileName.hashCode() * 127) % numPartitions);
        }
    }
}
