package com.roncoo.eshop.storm.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Component: Zookeeper session
 * Description:
 * Date: 17/7/27
 *
 * @author yue.zhang
 */
public class ZookeeperSession {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zooKeeper;

    public ZookeeperSession(){
        try {
            this.zooKeeper = new ZooKeeper(
                    "192.168.31.235:2181,192.168.31.184:2181,192.168.31.180:2181",
                    60000, new ZooKeeperWatcher());

            // 给一个状态CONNECTING，连接中
            System.out.println(this.zooKeeper.getState());

            try {
                // CountDownLatch
                // java多线程并发同步的一个工具类
                // 会传递进去一些数字，比如说1,2 ，3 都可以
                // 然后await()，如果数字不是0，那么久卡住，等待
                // 其他的线程可以调用coutnDown()，减1
                // 如果数字减到0，那么之前所有在await的线程，都会逃出阻塞的状态
                // 继续向下运行
                // 因为zookeeper初始化是异步的，所以要等到ZooKeeperWatcher的process做完之后才往下执行
                connectedSemaphore.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("ZooKeeper session established......");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class ZooKeeperWatcher implements Watcher{

        @Override
        public void process(WatchedEvent event) {
            System.out.println("Receive watched event: " + event.getState());
            if(Event.KeeperState.SyncConnected == event.getState()){
                connectedSemaphore.countDown();
            }
        }
    }

    public static void init(){
        ZookeeperSession.getInstance();
    }

    /**
     * 获取单例
     * @return
     */
    public static ZookeeperSession getInstance(){
        return Singleton.getInstance();
    }



    /**
     * 获取分布式锁
     */
    public void acquireDistributedLock(){
        String path = "/taskid-list-lock";

        try {
            zooKeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("success to acquire lock for taskid-list-lock");
        } catch (Exception e) {
            // 如果那个商品对应的锁的node已经存在了，就是已经被人加锁了，那么这里就会报错
            int count = 0;
            while (true){
                try {
//                    Thread.sleep(20); // 生产环境建议使用20
                    Thread.sleep(1000); // TODO 测试环境使用1000
                    zooKeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (Exception e1) {
                    count++;
                    System.out.println("the " + count + " times try to acquire lock for taskid-list-lock...");
                    continue;
                }
                System.out.println("success to acquire lock for taskid-list-lock after " + count + " times try...");
                break;
            }
        }
    }

    /**
     * 释放调一个分布式锁
     */
    public void releaseDistributeLock(){
        String path = "/taskid-list-lock";
        try {
            zooKeeper.delete(path,-1);
            System.out.println("release the lock for /taskid-list-lock...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 封装单例的内部静态类
     */
    private static class Singleton {

        private static ZookeeperSession instance;

        static {
            instance = new ZookeeperSession();
        }

        private static ZookeeperSession getInstance(){
            return instance;
        }
    }

    public String getNodeData(){
        try {
            String data = new String(zooKeeper.getData("/taskid-list",false,new Stat()));
            return data;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public void setNodeData(String path , String data){
        try {
            zooKeeper.setData(path,data.getBytes(),-1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
