package cn.downey.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        System.out.println("========= LogProcessor initialized at Time Millis " + System.currentTimeMillis());
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        //核心处理流程
        String input = new String(line);
        //提取数据，以固定前缀过滤日志信息
        if (input.contains("PRODUCT_RATING_PREFIX:")) {
            System.out.println("========= Product rating coming " + input + "at Time Millis " + System.currentTimeMillis());
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), input.getBytes());
        }


    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
