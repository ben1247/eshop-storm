package com.roncoo.eshop.storm;

import com.roncoo.eshop.storm.bolt.LogParseBolt;
import com.roncoo.eshop.storm.bolt.ProductCountBolt;
import com.roncoo.eshop.storm.spout.AccessLogKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Component: 热数据统计拓扑
 * Description:
 * Date: 17/8/12
 *
 * @author yue.zhang
 */
public class HotProductTopology {

    public static void main(String [] args){
        TopologyBuilder builder = new TopologyBuilder();

        // 这里的第一个参数的意思，就是给这个spout设置一个名字
        // 第二个参数的意思，就是创建一个spout的对象
        // 第三个参数的意思，就是设置spout的executor有几个
        builder.setSpout("AccessLogKafkaSpout",new AccessLogKafkaSpout(),1);

        // 第一个5代表设置了 5个executor ，setNumTasks(5) 代表设置了5个task。如果不设置task的话，task的默认数量和executor是一致的
        // 设置shuffleGrouping的目的是设置并行度，这里采用随机分组的策略，把spout采集到的数据随机分配到bolt中
        builder.setBolt("LogParseBolt",new LogParseBolt(),5).setNumTasks(5).shuffleGrouping("AccessLogKafkaSpout");

        // 设置fieldsGrouping的目的是设置流分组，意思是同一个field只能路由到同一个task中，不然统计会出现误差
        builder.setBolt("ProductCountBolt",new ProductCountBolt(),5).setNumTasks(10).fieldsGrouping("LogParseBolt",new Fields("productId"));

        Config config = new Config();

        // 说明是在命令行执行，打算提交到storm集群上去
        if(args != null && args.length > 0){
            // 启动3个worker去执行这个拓扑图
            config.setNumWorkers(3);

            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            // 说明是在idea里面本地运行

            config.setMaxTaskParallelism(20); // 设置一个最大的task平行度

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HotProductTopology",config,builder.createTopology());
            Utils.sleep(10000); // 让它跑个10秒
            cluster.shutdown();
        }


    }

}
