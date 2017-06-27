package com.sina.khsink;

import com.sina.utils.PropertiesUtils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by szq on 2017/6/26.
 * Kakfa客户端工具
 * 功能：
 *      1.连接kafka返回迭代器
 *      2.
 */
public class KafkaClient {

    private Properties props=null;
    private ConsumerConfig config=null;
    private ConsumerConnector consumer=null;
    private String topic=null;
    public KafkaClient(){
        init();
    }
    public void init(){
        this.props =PropertiesUtils.properties;
        this.config = new ConsumerConfig(props);
        this.consumer = Consumer.createJavaConsumerConnector(config);
        this.topic=PropertiesUtils.getString("sink.topic");
    }
    public ConsumerIterator<String, String>  consume() {

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        return stream.iterator();
    }

    public void close(){
        if (consumer!=null)
            consumer.shutdown();
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public ConsumerConfig getConfig() {
        return config;
    }

    public void setConfig(ConsumerConfig config) {
        this.config = config;
    }

    public ConsumerConnector getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerConnector consumer) {
        this.consumer = consumer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
