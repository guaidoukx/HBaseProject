package HDFSXH;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AttachHDFS {
    FileSystem fs;
    String rootPath;

    public AttachHDFS(String rootPath) throws  IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", rootPath);
        fs = FileSystem.newInstance(conf);
//        fs= FileSystem.get(new URI("hdfs://10.141.209.224:9000"),conf,"root");
        System.out.println("HDFS has been attached !");
        this.rootPath = rootPath;

    }


    /**
     * 用于测试 上传功能 ，源路径、目标路径都已经指定。
     * @throws IOException 一直不清楚。
     */
    public void put() throws IOException {//覆盖上传
        Path src = new Path("/Users/xiangyali/xyl.txt");
        Path dst = new Path("/test");
        fs.copyFromLocalFile(src, dst);
    }

    /**
     * 用于实际应用的 上传功能
     * @param srcString 原路径地址
     * @param dstString HDFS目标路径地址
     * @throws IOException 一直不清楚
     */
    public void put(String srcString, String dstString) throws IOException {//覆盖上传
        Path src = new Path(srcString);
        Path dst = new Path(dstString);
        fs.copyFromLocalFile(src, dst);
    }

    public void listPath(String path){
        try {
            if (fs.exists(new Path(path))) {
                if (!fs.isFile(new Path(path))) {
                    FileStatus[] fileStatus = fs.listStatus(new Path(path));
                    if (fileStatus.length != 0) {
                        for (FileStatus fileStatus1 : fileStatus) {
                            String pathResult = fileStatus1.getPath().toString();
                            String pattern = "^" + rootPath + "\\/(.*)";
                            Pattern filesAndPaths = Pattern.compile(pattern);
                            Matcher m = filesAndPaths.matcher(pathResult);
                            if (m.find()) {
                                System.out.print(m.group(1));
                                if (fileStatus1.isFile()) {
                                    System.out.println("\t is file.");
                                } else System.out.println("\t is directory.");
                            }
                        }
                    } else System.out.println("there is no more files in " + path + " directory");
                } else System.out.println(path + " is a file, not a directory.");
            } else System.out.println("no such file or directory! ");
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    //下载
    public void load() throws IOException {//覆盖下载
        Path src = new Path("G:/result2.txt");
        Path dst = new Path("/test/result.txt");
        fs.copyToLocalFile(dst, src);
    }

    //新建文件夹
    public void add() throws Exception {
        fs.mkdirs(new Path("/path"));
    }


    //删除文件或文件夹，bollean代表是否递归删除
    public void delete() throws IOException {
        fs.delete(new Path("/result.txt"),true);

    }


    public static void main(String args[]) throws InterruptedException, IOException, URISyntaxException {
        AttachHDFS attachHDFS = new AttachHDFS("hdfs://10.141.209.224:9000");
        attachHDFS.listPath("/");
    }


}
