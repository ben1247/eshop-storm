package com.roncoo.eshop.storm.bolt;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Component: 日志解析的bolt
 * 每个bolt代码，同样是发送到worker某个executor的task里面去运行
 * Description:
 * Date: 17/8/12
 *
 * @author yue.zhang
 */
public class LogParseBolt extends BaseRichBolt{

    // 是Bolt对tuple的发射器
    private OutputCollector collector;

    /**
     * 对于bolt来说，第一个方法，就是prepare方法
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 每次接收到一条数据后，就会交给这个executor方法来执行
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String message = tuple.getStringByField("message");
        JSONObject messageJSON = JSONObject.parseObject(message);
        JSONObject uriArgsJSON = messageJSON.getJSONObject("uri_args");
        Long productId = uriArgsJSON.getLong("productId");

        if(productId != null){
            collector.emit(new Values(productId));
        }
    }

    /**
     * 定义发射出去的tuple,每个field的名称
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("productId"));
    }
}
