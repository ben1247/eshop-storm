package com.roncoo.eshop.storm.spout;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Component: kafka消费数据的spout（spout就是数据源组件），
 * 启动一个kafka的consumer，将消息放到queue中，再由nextTuple去消费queue，发射出去
 * Description:
 * Date: 17/8/12
 *
 * @author yue.zhang
 */
public class AccessLogKafkaSpout extends BaseRichSpout{

    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(1000); // 最多放1000条;

    private SpoutOutputCollector collector;

    /**
     * open方法，是对spout进行初始化的
     * 比如说，创建一个线程池，或者创建一个数据库连接池，或者构造一个httpclient
     * 这里就是创建一个kafka消费者的链接初始化
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // 在open方法初始化的时候，会传入进来一个东西，叫做SpoutOutputCollector
        // 这个SpoutOutputCollector就是用来发射数据出去的
        this.collector = collector;
        // 启动kafka消费者
        startKafkaConsumer();
    }

    /**
     * 这个spout类，最终会运行在task中，某个worker进程的某个executor线程内部的某个task中
     * 那个task会负责去不断的无限循环调用nextTuple()方法
     * 无限循环调用，可以不断发射最新的数据出去，形成一个数据流
     */
    @Override
    public void nextTuple() {
        if(queue.size() > 0){
            try {
                String message = queue.take();
                // 这个values，你可以认为就是构建一个tuple
                // tuple是最小的数据单位，无限个tuple组成的流就是一个stream
                collector.emit(new Values(message));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else {
            // 防止数据发射过快
            Utils.sleep(100);
        }
    }

    /**
     * 这个方法是定义一个你发射出去的每个tuple中的每个field的名称是什么
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    private void startKafkaConsumer(){

        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.31.235:2181,192.168.31.184:2181,192.168.31.180:2181");
        props.put("group.id", "eshop-cache-group");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        String topic = "access-log";
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1); // 打算用几个线程去消费这个topic
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        for (KafkaStream stream : streams) {
            new Thread(new KafkaMessageProcessor(stream)).start();
        }
    }

    private class KafkaMessageProcessor implements Runnable{

        private KafkaStream kafkaStream;

        public KafkaMessageProcessor(KafkaStream kafkaStream){
            this.kafkaStream = kafkaStream;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[],byte[]> it = kafkaStream.iterator();
            while (it.hasNext()){
                String message = new String(it.next().message());
                try {
                    queue.put(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
