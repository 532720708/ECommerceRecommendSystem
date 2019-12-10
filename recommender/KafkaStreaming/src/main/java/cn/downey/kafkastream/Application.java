package cn.downey.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        String brokers = "hadoop100:9092";
        String zookeepers = "hadoop100:2181";

        //定义输入和输出的topic
        String from = "log";
        String to = "recommender";

        //定义kafka配置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        //创建kafka stream配置对象
        StreamsConfig config = new StreamsConfig(settings);

        // 拓扑建构器
        TopologyBuilder builder = new TopologyBuilder();

        // 定义流处理的拓扑结构
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESS", LogProcessor::new, "SOURCE")
                .addSink("SINK", to, "PROCESS");

        //创建kafka stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

    }
}
