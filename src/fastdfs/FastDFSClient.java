package fastdfs;

import org.csource.fastdfs.StorageClient1;
import org.csource.fastdfs.TrackerServer;

/**
 * 客户端
 *
 * @author clawhub
 */
public class FastDFSClient {

    /**
     * 连接池
     */
    private FastDFSConnPool fastDFSConnPool;

    /**
     * Instantiates a new Fast dfs client.
     *
     * @param fastDFSConnPool the fast dfs conn pool
     */
    public FastDFSClient(FastDFSConnPool fastDFSConnPool) {
        this.fastDFSConnPool = fastDFSConnPool;
    }

    @FunctionalInterface
    public interface CallBack<T> {

        /**
         * 执行方法
         *
         * @param storageClient storageClient
         * @return
         * @throws Exception
         */
        T invoke(StorageClient1 storageClient) throws Exception;
    }

    /**
     * 执行方式
     *
     * @param <T>    the type parameter
     * @param invoke the invoke
     * @return the t
     */
    public <T> T processFdfs(CallBack<T> invoke) {
        TrackerServer trackerServer = null;
        T t;
        try {
            //获取tracker连接
            trackerServer = fastDFSConnPool.checkOut();
            //获取storage
            StorageClient1 storageClient = new StorageClient1(trackerServer, null);
            //执行操作
            t = invoke.invoke(storageClient);
            //释放连接
            fastDFSConnPool.checkIn(trackerServer);
            return t;
        } catch (Exception e) {
            //删除链接
            fastDFSConnPool.drop(trackerServer);
            throw new RuntimeException(e);
        }
    }
}
