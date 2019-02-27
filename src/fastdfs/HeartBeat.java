package fastdfs;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.csource.fastdfs.ProtoCommon;
import org.csource.fastdfs.TrackerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 连接池定时器设置
 *
 * @author clawhub
 */
class HeartBeat {

    /**
     * 日志记录器
     */
    private Logger logger = LoggerFactory.getLogger(HeartBeat.class);

    /**
     * fastdfs连接池
     */
    private FastDFSConnPool fastDFSConnPool;

    /**
     * 构造
     *
     * @param fastDFSConnPool the fast dfs conn pool
     */
    HeartBeat(FastDFSConnPool fastDFSConnPool) {
        this.fastDFSConnPool = fastDFSConnPool;
    }

    /**
     * 心跳任务
     */
    private class HeartBeatTask implements Runnable {

        @Override
        public void run() {
            LinkedBlockingQueue<TrackerServer> idleConnectionPool = fastDFSConnPool.getIdleConnectionPool();
            TrackerServer ts = null;
            for (int i = 0; i < idleConnectionPool.size(); i++) {
                try {
                    ts = idleConnectionPool.poll(fastDFSConnPool.getWaitTimes(), TimeUnit.SECONDS);
                    if (ts != null) {
                        ProtoCommon.activeTest(ts.getSocket());
                        idleConnectionPool.add(ts);
                    } else {
                        //代表已经没有空闲长连接
                        break;
                    }
                } catch (Exception e) {
                    //发生异常,要删除，进行重建
                    logger.error("heart beat conn  have dead, and reconnect.", e);
                    fastDFSConnPool.drop(ts);
                }
            }

        }
    }

    /**
     * 定时执行任务，检测当前的空闲连接是否可用
     */
    void beat() {
        logger.debug("check heart beat status : [{}].", System.currentTimeMillis());
        //org.apache.commons.lang3.concurrent.BasicThreadFactory
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("example-schedule-pool-%d").daemon(true).build());
        executorService.scheduleAtFixedRate(new HeartBeatTask(), 0L, 0, TimeUnit.MINUTES);
    }

}
