package com.calabar.flinkDemo.kafka;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * <p/>
 * <li>@author: jyj019 </li>
 * <li>Date: 2018/9/17 15:38</li>
 * <li>@version: 2.0.0 </li>
 * <li>@since JDK 1.8 </li>
 */
public class WriteIntoKafka {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map properties= new HashMap();
        properties.put("bootstrap.servers", "192.168.10.63:6667,192.168.10.64:6667,192.168.10.65:6667");
        properties.put("group.id", "t10");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("topic", "kks-topic-FFT");
        //KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        // parse user parameters
        //ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromMap(properties);

        // add a simple source which is writing some strings
        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());

        // write stream to Kafka
        messageStream.addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("bootstrap.servers"),
                parameterTool.getRequired("topic"),
                new SimpleStringSchema()));

        messageStream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        messageStream.print();

        env.execute();
    }

    public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            //int i=0;
            while(running) {
                //i++;
                ctx.collect(prouderJson());
                //System.out.println(prouderJson());

            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static String prouderJson() throws Exception {
        //  long start = System.currentTimeMillis();
        Integer value;
        String[] channels = new String[]{"000000007946", "000000007947","000000007948","000000007949","000000007950","000000007951","000000007952","000000007953",
                "000000007954","000000007955","000000007956","000000007957","000000007958","000000007959","000000007960","000000007961","000000007966",
                "000000007967","000000007968","000000007969","000000007970","000000007971","000000007986","000000007987"};
        StringBuffer json = new StringBuffer();
        json.append("{\n" + "    \"header\": {\n" + "        \"head\": \"EB90EB90EB90\",\n" + "        \"plant_code\": 1,\n" + "        " +
                "\"set_code\": 1,\n" + "        \"device_type\": 1,\n" + "        \"time\": "+System.currentTimeMillis() +",\n"
                + "        \"data_length\": 4999\n" + "    },\n" + "    \"base_info\": {\n" + "        \"work_identity\": 1,\n" +
                "        \"sample_points_per_cycle\": 1024,\n" + "        \"sampling_period\": 8,\n" + "        \"sampling_number\": 1024,\n" +
                "        \"rotate_speed\": "+randmomUtils1(RandomUtils.nextInt(0, 3)) +",\n" + "        \"fast_variable_channels\": 24\n" + "    },\n \"channel\":{");
        for (int i=0;i<23;i ++) {
            json.append("\"" +  channels[i] + "\":{\"peak\":" + randmomUtils2(RandomUtils.nextInt(0, 10)) + ",\n"
                    + "\"phase_1x\":" + RandomUtils.nextFloat(0, 500) + ",\n"
                    + "\"amplitude_1x\":" + (RandomUtils.nextFloat( 0, (float) 6.28)-3.14) + ",\n"
                    + "\"phase_2x\":" + RandomUtils.nextFloat(0, 50) + ",\n"
                    + "\"amplitude_2x\":" + (RandomUtils.nextFloat(0, (float) 6.28)-3.14) + ",\n"
                    + "\"half_amplitud\":" + RandomUtils.nextFloat(0, 50) + ",\n"
                    + "\"voltage\":" + RandomUtils.nextFloat(0, 5) + ",\n"
                    +"\"waveform_data\":[");
            for(int j=1;j<1024;j ++){
                value = (int) (5 * Math.sin(360 / 32 * j) + (8 * (Math.sin((360 / 64) * j))));
                json.append(value+",");
            }
            value=(int)(5*(Math.sin((360/32)*1024))+8*(Math.sin((360/64)*1024)));
            json.append(value+"]},\n");
        }
        json.append("\""+channels[23] +"\":{\"peak\":" + randmomUtils1(RandomUtils.nextInt(0, 10)) + ",\n"
                + "\"phase_1x\":" + RandomUtils.nextFloat(0, 500) + ",\n"
                + "\"amplitude_1x\":" + (RandomUtils.nextFloat( 0, (float) 6.28)-3.14) + ",\n"
                + "\"phase_2x\":" + RandomUtils.nextFloat(0, 50) + ",\n"
                + "\"amplitude_2x\":" + (RandomUtils.nextFloat(0, (float) 6.28)-3.14) + ",\n"
                + "\"half_amplitud\":" + RandomUtils.nextFloat(0, 50) + ",\n"
                + "\"voltage\":" + RandomUtils.nextFloat(0, 5) + ",\n"
                +"\"waveform_data\":[");
        for(int j=1;j<1024;j ++){
            value=(int)(5*(Math.sin((360/32)*j))+8*(Math.sin((360/64)*j)));
            json.append(value+",");
        }
        value=(int)(5*(Math.sin((360/32)*1024))+8*(Math.sin((360/64)*1024)));
        json.append(value+"]}}}\n");
        //  long end = System.currentTimeMillis();
        // LOGGER.info("制造数据，耗时：-->"+(start-end) );
        return String.valueOf(json);
    }

    public static Float randmomUtils1(int i) throws Exception{
        Float value=RandomUtils.nextFloat(2950, 3080);
        switch (i){
            case 1:
                value=RandomUtils.nextFloat(10, 90);
                break;
            case 2:
                value=RandomUtils.nextFloat(0, 80);
                break;
        }
        return value;
    }

    public static Float randmomUtils2(int i) throws Exception{
        Float value=RandomUtils.nextFloat(290, 300);
        switch (i){
            case 1:
                value=RandomUtils.nextFloat(0, 200);
                break;
        }
        return value;
    }

//    public static class SimpleStringSchema implements DeserializationSchema<String>, SerializationSchema<String, byte[]> {
//        private static final long serialVersionUID = 1L;
//
//        public SimpleStringSchema() {
//        }
//
//        public String deserialize(byte[] message) {
//            return new String(message);
//        }
//
//        public boolean isEndOfStream(String nextElement) {
//            return false;
//        }
//
//        public byte[] serialize(String element) {
//            return element.getBytes();
//        }
//
//        public TypeInformation<String> getProducedType() {
//            return TypeExtractor.getForClass(String.class);
//        }
//    }
}
