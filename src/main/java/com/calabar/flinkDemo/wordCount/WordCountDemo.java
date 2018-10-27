package com.calabar.flinkDemo.wordCount;

import com.calabar.flinkDemo.wordCount.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.util.Collector;

/**
 * <p/>

 * <li>@author: jyj019</li>
 * <li>Date: 2018/9/3 10:05</li>
 * <li>@version: 2.0.0 </li>
 * <li>@since JDK 1.8 </li>
 */
public class WordCountDemo {

        public static void main(String[] args) throws Exception {
            final ParameterTool params = ParameterTool.fromArgs(args);

            // create execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setGlobalJobParameters(params);

            // get input data
            DataSet<String> text;
            if (params.has("input")) {
                // read the text file from given input path
                text = env.readTextFile(params.get("input"));
            } else {
                // get default test text data
                System.out.println("Executing WordCount example with default input data set.");
                System.out.println("Use --input to specify file input.");
                text = WordCountData.getDefaultTextLineDataSet(env);
            }

            DataSet<Tuple2<String, Integer>> counts =
                    // split up the lines in pairs (2-tuples) containing: (word,1)
                    text.flatMap(new Tokenizer())
                            // group by the tuple field "0" and sum up tuple field "1"
                            .groupBy(0)
                            .sum(1);

            // emit result
            if (params.has("output")) {
                counts.writeAsCsv(params.get("output"), "\n", " ");
                // execute program
                env.execute("WordCount Example");
            } else {
                System.out.println("Printing result to stdout. Use --output to specify output path.");
                counts.print();
            }

        }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
