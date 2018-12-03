import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class TFIDFMapReduce {
    static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

        /*
        * map input: word:filename, tf*idf
        *     output: filename, tf*idf
        * */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            context.write(new Text(tokenizer.nextToken().split(":")[1]), new Text(tokenizer.nextToken()));
        }
    }

    static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

        double tfidf = 0d;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null){
                return;
            }


            for (Text value : values){
                tfidf += Double.parseDouble(value.toString());
            }

            context.write(key, new Text(String.valueOf(tfidf)));


        }
    }
}