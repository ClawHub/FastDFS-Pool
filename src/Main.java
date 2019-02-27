import fastdfs.FastDFSClient;
import fastdfs.FastDFSConnPool;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.FileInfo;

import java.io.IOException;

/**
 * <Description> Main <br>
 *
 * @author clawhub<br>
 */
public class Main {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
        //初始化连接池
        FastDFSConnPool fastDFSConnPool = new FastDFSConnPool()
                .confFileName("./config/fdfs_client.conf")
                .maxPoolSize(8)
                .minPoolSize(1)
                .reConnNum(2)
                .waitTimes(2).build();

        //使用客户端
        FastDFSClient client = new FastDFSClient(fastDFSConnPool);
        //上传  ileName 文件全路径 extName 文件扩展名，不包含（.） metas 文件扩展信息
        String parts = client.processFdfs(storageClient -> storageClient.upload_file1("fileName", "extName", new NameValuePair[0]));
        //下载 fileId: group1/M00/00/00/wKgRsVjtwpSAXGwkAAAweEAzRjw471.jpg
        byte[] bytes = client.processFdfs(storageClient -> storageClient.download_file1("fileId"));
        //删除 -1失败,0成功
        int result = client.processFdfs(storageClient -> storageClient.delete_file1("fileId"));
        //获取远程服务器文件资源信息  groupName   文件组名 如：group1  remoteFileName M00/00/00/wKgRsVjtwpSAXGwkAAAweEAzRjw471.jpg
        FileInfo fileInfo = client.processFdfs(storageClient -> storageClient.get_file_info("groupName", "remoteFileName"));
    }
}
