package fastdfs;

import org.csource.common.MyException;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.ProtoCommon;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * FastDFS连接池
 * @author clawhub
 */
public class FastDFSConnPool {

    /**
     * The Logger.
     */
    private Logger logger = LoggerFactory.getLogger(FastDFSConnPool.class);
    /**
     * 重新连接次数
     */
    private int reConnNum = 5;

    /**
     * 空闲的连接池
     */
    private LinkedBlockingQueue<TrackerServer> idleConnectionPool;

    /**
     * 连接池最小连接数
     */
    private long minPoolSize;

    /**
     * 连接池最大连接数
     */
    private int maxPoolSize = 8;

    /**
     * 默认等待时间（单位：秒）
     */
    private long waitTimes;

    /**
     * 配置文件路径
     */
    private String confFileName;

    /**
     * 创建TrackerServer
     *
     * @return the tracker server
     */
    private TrackerServer createTrackerServer() {
        logger.info("create tracker server.");
        TrackerServer trackerServer = null;
        try {
            TrackerClient trackerClient = new TrackerClient();
            //获取连接
            trackerServer = trackerClient.getConnection();
            //如果获取的连接为空，重试
            int flag = 1;
            while (trackerServer == null && flag < reConnNum) {
                logger.info("recreate tracker server times [{}].", flag);
                flag++;
                trackerServer = trackerClient.getConnection();
            }
            if (null != trackerServer) {
                //测试连通性
                ProtoCommon.activeTest(trackerServer.getSocket());
            }
        } catch (Exception e) {
            logger.error("create tarcker server failed.", e);
        } finally {
            if (trackerServer != null) {
                try {
                    //将连接关闭
                    trackerServer.close();
                } catch (Exception e) {
                    logger.error("create tracker serverm, close tracker server exception.", e);
                }
            }
        }
        return trackerServer;
    }

    /**
     * Description: 获取空闲连接<br>
     * 1).在空闲池（idleConnectionPool)中弹出一个连接； <br>
     * 2).把该连接放入忙碌池（busyConnectionPool）中; <br>
     * 3).返回 connection <br>
     * 4).如果没有idle connection, 等待wait_time秒, and check again <br>
     *
     * @return <br>
     */
    public TrackerServer checkOut() {

        logger.info("check out idle connection.");
        TrackerServer trackerServer;
        try {
            trackerServer = idleConnectionPool.poll(waitTimes, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("checkOut TrackerServer fail.");
            throw new RuntimeException("checkOut TrackerServer fail.");
        }

        return trackerServer;

    }

    /**
     * Description: 释放繁忙连接 <br>
     * 1).如果空闲池的连接小于最小连接值，就把当前连接放入idleConnectionPool； <br>
     * 2).如果空闲池的连接等于或大于最小连接值，就把当前释放连接丢弃； <br>
     *
     * @param trackerServer 需释放的连接对象
     */
    public void checkIn(TrackerServer trackerServer) {
        logger.info("release prams:{}.", trackerServer);
        if (trackerServer != null) {
            //有地方就放里面，如果没有就删掉
            if (!idleConnectionPool.offer(trackerServer)) {
                //删除不可用链接
                drop(trackerServer);
            }

        }

    }

    /**
     * Description: 删除不可用的连接，并把当前连接数减一<br>
     *
     * @param trackerServer <br>
     */
    public void drop(TrackerServer trackerServer) {
        logger.debug("drop invalid connection parms:{}.", trackerServer);
        if (trackerServer != null) {
            try {
                trackerServer.close();
            } catch (IOException e) {
                logger.error("drop invalid connection failed.", e);
            }
        }
    }

    /**
     * 连接池最小连接数
     *
     * @param minPoolSize the min pool size
     * @return the fast dfs conn pool
     */
    public FastDFSConnPool minPoolSize(long minPoolSize) {
        this.minPoolSize = minPoolSize;
        return this;
    }


    /**
     * 连接池最大连接数
     *
     * @param maxPoolSize the max pool size
     * @return the fast dfs conn pool
     */
    public FastDFSConnPool maxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    /**
     * 重新连接次数
     *
     * @param reConnNum the re conn num
     * @return the fast dfs conn pool
     */
    public FastDFSConnPool reConnNum(int reConnNum) {
        this.reConnNum = reConnNum;
        return this;
    }

    /**
     * 默认等待时间（单位：秒）
     *
     * @param waitTimes the wait times
     * @return the fast dfs conn pool
     */
    public FastDFSConnPool waitTimes(int waitTimes) {
        this.waitTimes = waitTimes;
        return this;
    }

    /**
     * 配置文件路径
     *
     * @param confFileName the conf file name
     * @return the fast dfs conn pool
     */
    public FastDFSConnPool confFileName(String confFileName) {
        this.confFileName = confFileName;
        return this;
    }

    /**
     * Description: 获取空闲连接池<br>
     *
     * @return the idle connection pool
     */
    public LinkedBlockingQueue<TrackerServer> getIdleConnectionPool() {
        return idleConnectionPool;
    }

    /**
     * Build fast dfs conn pool.
     *
     * @return the fast dfs conn pool
     */
    public FastDFSConnPool build() {
        // 初始化空闲连接池
        idleConnectionPool = new LinkedBlockingQueue<>(maxPoolSize);

        //初始化全局参数
        try {
            ClientGlobal.init(confFileName);
        } catch (IOException | MyException e) {
            throw new RuntimeException("init client global exception.", e);
        }

        // 往线程池中添加默认大小的线程
        TrackerServer trackerServer;
        for (int i = 0; i < minPoolSize; i++) {
            //获取到连接
            trackerServer = createTrackerServer();
            if (trackerServer != null) {
                //放入空闲池
                idleConnectionPool.offer(trackerServer);
            }
        }
        // 注册心跳
        new HeartBeat(this).beat();

        return this;
    }

    /**
     * Gets wait times.
     *
     * @return the wait times
     */
    public long getWaitTimes() {
        return waitTimes;
    }
}
