package HDFSXH;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AttachHDFS {
    FileSystem fs;
    String rootPath;

    public AttachHDFS(String rootPath) throws  IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", rootPath);
        fs = FileSystem.newInstance(conf);
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
    public boolean put(String srcString, String dstString) throws IOException {//覆盖上传
        Path src = new Path(srcString);
        if (!fs.exists(src)){
            Path dst = new Path(dstString);
            fs.copyFromLocalFile(src, dst);
            return true;
        }else return false;
    }

    public void listPath(String path){
        try {
            if (fs.exists(new Path(path))) {
                if (!fs.isFile(new Path(path))) {
                    FileStatus[] fileStatus = fs.listStatus(new Path(path));
                    if (fileStatus.length != 0) {
                        for (FileStatus fileStatus1 : fileStatus) {
                            System.out.println(fileStatus1);
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
        Path src = new Path("/Users/xiangyali/Desktop/Kmeans.ja");
        Path dst = new Path("/xyl/Kmeans.jar");
        fs.copyToLocalFile(dst, src);
    }


    //新建文件夹
    public boolean makeDirectory(String directoryName) throws Exception {
        Path directoryPath = new Path( directoryName);
        if (fs.exists(directoryPath)) {
            System.out.println("The directory is exist.");
            return false;
        }else {
            System.out.println(directoryName + "created successfully!");
            return fs.mkdirs(directoryPath);
        }
    }


    public boolean createFile(String fileName) throws IOException {
        Path filePath = new Path(fileName);
        fs.create(filePath);
        if (fs.exists(filePath)){
            System.out.println(fileName + " has been created successfully!" );
            return true;
        }
        else {
            System.out.println(fileName + " can't be created!" );
            return false;
        }
    }


    //删除文件或文件夹，bollean代表是否递归删除
    public boolean delete(String fileName) throws IOException {
        Path filePath = new Path(rootPath + fileName);
        if (fs.exists(filePath)){
            return fs.delete(filePath, true);
        }else {
            System.out.println("No such a file or directory.");
            return false;
        }
    }


    public static void main(String args[]) throws Exception {
        AttachHDFS attachHDFS = new AttachHDFS("hdfs://cloud024:9000");
//        attachHDFS.listPath("/xyl");
//        attachHDFS.makeDirectory("/xyl/abc");
//        attachHDFS.put("/Users/xiangyali/Desktop/Hadoop.ppt","/xyl/abc" );
//        attachHDFS.createFile("/xyl/aaa/xyl1.txt");
//        attachHDFS.createFile("/xyl/aaa/xyl2.txt");
//        attachHDFS.delete("/xyl/aaa");
        attachHDFS.load();
    }


}
