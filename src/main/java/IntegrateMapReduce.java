import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class IntegrateMapReduce {
    static class IntegrateMapper extends Mapper<Object, Text, Text, Text> {

        /*
         * map input: result of tf(word:filename, tf) and idf(word:!, idf)
         *     output: (word:!, idf)
         *             (word:filename, tf)
         *
         * */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            context.write(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
        }
    }

    /*
    * reduce input: (word:!, idf)
     *              (word:filename1, tf)
     *              (word:filename2, tf)
     *              (word:filename3, tf)
     *       output: word:filename, tf*idf
    * */
    static  class IntegrateReducer extends Reducer<Text, Text, Text, Text> {

        private double keywordIDF = 0.0d;
        private Text value = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }

            if (key.toString().split(":")[1].startsWith("!")) {
                keywordIDF = Double.parseDouble(values.iterator().next().toString());
                return;
            }

            value.set(String.valueOf(Double.parseDouble(values.iterator().next().toString()) * keywordIDF));

            context.write(key, value);

        }
    }
}
