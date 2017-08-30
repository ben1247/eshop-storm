package com.roncoo.eshop.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.roncoo.eshop.storm.zk.ZookeeperSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Component: 商品访问次数统计bolt
 * Description:
 * Date: 17/8/12
 *
 * @author yue.zhang
 */
public class ProductCountBolt extends BaseRichBolt {


    private LRUMap<Long,Long> productCountMap = new LRUMap<>(1000);

    private ZookeeperSession zkSession;

    private int taskid;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.zkSession = ZookeeperSession.getInstance();
        this.taskid = context.getThisTaskId();

        new Thread(new ProductCountThread()).start();

        // 1、将自己的taskid写入一个zookeeper node中，形成taskid的列表
        // 2、然后每次都将自己的热门商品列表，写入自己的taskid对应的zookeeper节点
        // 3、然后这样的话，并行的预热程序才能从第一步中知道，有哪些taskid
        // 4、然后并行预热程序根据每个taskid去获取一个锁，然后再从对应的znode中拿到热门商品列表
        initTaskId(context.getThisTaskId());
    }

    private void initTaskId(int taskid){
        // ProductCountBolt所有的task启动的时候，都会将自己的taskid写到同一个node的值中
        // 格式就是逗号分隔，拼接成一个列表
        zkSession.acquireDistributedLock();
        String taskidList = zkSession.getNodeData();
        if(!"".equals(taskidList)){
            taskidList += "," + taskid;
        }else {
            taskidList += taskid;
        }
        zkSession.setNodeData("/taskid-list",taskidList);

        zkSession.releaseDistributeLock();
    }

    @Override
    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");
        Long count = productCountMap.get(productId);
        if(count == null){
            count = 0L;
        }
        productCountMap.put(productId,++count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private class ProductCountThread implements Runnable{

        @Override
        public void run() {

            List<Map.Entry<Long,Long>> topNProductList = new ArrayList<>();

            int topN = 3;

            while (true){
                topNProductList.clear();

                if (productCountMap.size() == 0){
                    Utils.sleep(100);
                    continue;
                }

                for (Map.Entry<Long , Long> productCountEntry : productCountMap.entrySet()){
                    if(topNProductList.size() == 0){
                        topNProductList.add(productCountEntry);
                    }else {
                        // 比较大小，生成最热topN的算法
                        boolean bigger = false;
                        for(int i = 0; i < topNProductList.size(); i++){
                            Map.Entry<Long, Long> topnProductCountEntry = topNProductList.get(i);

                            if(productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                int lastIndex = topNProductList.size() < topN ? topNProductList.size() - 1 : topN - 2;
                                for(int j = lastIndex; j >= i; j--) {
                                    topNProductList.set(j + 1, topNProductList.get(j));
                                }
                                topNProductList.set(i, productCountEntry);
                                bigger = true;
                                break;
                            }
                        }

                        if(!bigger) {
                            if(topNProductList.size() < topN) {
                                topNProductList.add(productCountEntry);
                            }
                        }
                    }

                }

                // 获取一个topN list
                String topNProductListJSON = JSONArray.toJSONString(topNProductList);
                zkSession.setNodeData("/task-hot-product-list-" + taskid , topNProductListJSON);

                Utils.sleep(5000);
            }
        }
    }
}
