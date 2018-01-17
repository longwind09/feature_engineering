package org.felix.ml.fe.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringBufferInputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 *
 */
public class HadoopUtil {
    private static Logger warn = Logger.getLogger("warn");

    public static void initConf(Configuration conf, String parameters, String[] requires) {
        String[] runArgs = parameters.split("&");
        for (String rarg : runArgs) {
            String[] keyAndVal = rarg.split("=");
            if (keyAndVal.length == 2 && !StringUtils.isEmpty(keyAndVal[0]) && !StringUtils.isEmpty(keyAndVal[1]))
                conf.set(StringUtils.trim(keyAndVal[0]), StringUtils.trim(keyAndVal[1]));
        }
        conf.set(Constant.CONF_ARGS, parameters);
        if (requires != null) {
            for (String str : requires) {
                if (conf.get(str) == null) {
                    warn.error("lost configure " + str);
                    System.exit(-1);
                }
            }
        }
    }

    public static List<Path> setInput(Configuration conf, Job job, String input) throws IOException {
        List<Path> ret = listInputPath(conf, job, input);
        setInputPath(job, ret);
        return ret;
    }

    /**
     * @param conf
     * @param job
     * @param input
     * @return
     * @throws IOException 把输入路径解析成list,并变成hdfs路径
     */
    public static List<Path> listInputPath(Configuration conf, Job job, String input) throws IOException {
        String[] paths = input.split(";");
        FileSystem fstm = FileSystem.get(conf);
        List<Path> ret = new ArrayList<Path>();
        boolean hasInput = false;
        for (String path : paths) {
            if (StringUtils.isEmpty(path))
                continue;
            path = StringUtils.trim(path);
            Path dir = new Path(path + "/");
            if (fstm.exists(dir)) {
                hasInput = true;
                ret.add(new Path(path));
            } else {
                System.err.println(String.format("path:[%s] not exist!", path));
                System.exit(-1);
            }
        }
        if (!hasInput) {
            System.err.println("no input file!");
            System.exit(-1);
        }
        return ret;
    }

    public static void setInputPath(Job job, List<Path> paths) throws IOException {
        for (Path path : paths) {
            FileInputFormat.addInputPath(job, path);
        }
    }

    public static List<Path> setInputWithMapperRecursion(Configuration conf, Job job, List<Path> paths,
                                                         Class mapperClass, boolean includeDir, boolean includeFile) throws IOException {
        List<Path> ret = listFileRecursion(conf, paths, false, true);
        for (Path apath : ret) {
            MultipleInputs.addInputPath(job, apath, TextInputFormat.class, mapperClass);
        }
        return ret;
    }

    public static List<Path> setInputWithMapperRecursion(Configuration conf, Job job, Path path, Class mapperClass,
                                                         boolean includeDir, boolean includeFile) throws IOException {
        List<Path> ret = listFileRecursion(conf, path, true, false);
        for (Path apath : ret) {
            MultipleInputs.addInputPath(job, apath, TextInputFormat.class, mapperClass);
        }
        return ret;
    }

    public static void setInputWithMapper(Job job, List<Path> paths, Class mapperClass) throws IOException {
        for (Path path : paths) {
            MultipleInputs.addInputPath(job, path, TextInputFormat.class, mapperClass);
        }
    }

    public static List<Path> listFile(Configuration conf, String multiPath, boolean includeDir, boolean includeFile)
            throws IOException {
        String[] arrs = StringUtils.split(multiPath, ";");
        return listFile(conf, arrs, includeDir, includeFile);
    }

    public static List<Path> listFile(Configuration conf, List<String> paths, boolean includeDir, boolean includeFile)
            throws IOException {
        List<Path> ret = new ArrayList();
        for (String path : paths) {
            if (StringUtils.isEmpty(path))
                continue;
            Path dir = new Path(path);
            ret.addAll(listFile(conf, dir, includeDir, includeFile));
        }
        return ret;
    }

    public static List<Path> listFile(Configuration conf, String[] paths, boolean includeDir, boolean includeFile)
            throws IOException {
        List<Path> ret = new ArrayList();
        for (String path : paths) {
            if (StringUtils.isEmpty(path))
                continue;
            Path dir = new Path(path);
            ret.addAll(listFile(conf, dir, includeDir, includeFile));
        }
        return ret;
    }

    public static List<Path> listFile(Configuration conf, Path dir, boolean includeDir, boolean includeFile)
            throws IOException {
        List<Path> ret = new ArrayList();
        FileSystem fstm = FileSystem.get(conf);
        if (fstm.isFile(dir)) {
            if (StringUtils.startsWith(dir.getName(), "_"))
                return ret;
        } else if (fstm.exists(dir)) {
            FileStatus[] fileStatus = fstm.listStatus(dir);
            for (FileStatus fStatus : fileStatus) {
                if (StringUtils.startsWith(fStatus.getPath().getName(), "_"))
                    continue;
                if (fStatus.isDirectory() && includeDir) {
                    ret.add(fStatus.getPath());
                } else if (fStatus.isFile() && includeFile) {
                    ret.add(fStatus.getPath());
                }
            }
        }
        return ret;
    }

    public static List<Path> listFileRecursion(Configuration conf, Path dir, boolean includeDir, boolean includeFile)
            throws IOException {
        List<Path> ret = new ArrayList<Path>();
        ret.add(dir);
        List<Path> subPaths = listFile(conf, dir, includeDir, includeFile);
        ret.addAll(listFileRecursion(conf, subPaths, includeDir, includeFile));
        return ret;
    }

    public static List<Path> listFileRecursion(Configuration conf, List<Path> paths, boolean includeDir,
                                               boolean includeFile) throws IOException {
        List<Path> ret = new ArrayList<Path>();
        for (Path path : paths) {
            ret.addAll(listFileRecursion(conf, path, includeDir, includeFile));
        }
        return ret;
    }

    public static List<Path> setRTCTRInput(Configuration conf, Job job, String inDir, List<String> times,
                                           Class mapperClass, List<Path> tmp) throws IOException {
        List<Path> paths = listRtCTRFiles(conf, inDir, times, tmp);
        setInputWithMapper(job, paths, mapperClass);
        return paths;
    }

    public static List<Path> listRtCTRFiles(Configuration conf, String path, List<String> times, List<Path> tmp)
            throws IOException {
        List<Path> files = listFile(conf, path, false, true);
        List<Path> ret = new ArrayList<Path>();
        for (Path afile : files) {
            String name = afile.getName();
            if (!contains(name, times)) {
                continue;
            }
            if (StringUtils.endsWith(name, ".tmp")) {
                tmp.add(afile);
                continue;
            }
            ret.add(afile);
        }
        return ret;
    }

    public static List<Path> setRtCTRCntFileList(Configuration conf, Job job, String path, List<String> times,
                                                 List<String> dates) throws IOException {
        Set<String> dateSet = new HashSet<String>();
        String end = times.get(0);
        String start = times.get(times.size() - 1);
        List<Path> ret = new ArrayList<Path>();
        for (String date : dates) {
            if (dateSet.contains(date))
                continue;
            dateSet.add(date);
            List<Path> dirs = listFile(conf, String.format("%s/%s", path, date), true, false);
            for (Path apath : dirs) {
                String[] arrs = StringUtils.split(apath.toUri().getPath(), "/");
                String name = apath.getName();
                if (name.compareTo(start) >= 0 && name.compareTo(end) <= 0) {
                    ret.add(apath);
                }
            }
        }
        setInputPath(job, ret);
        return ret;
    }

    private static boolean contains(String str, List<String> searchStrs) {
        boolean ret = false;
        for (String searchStr : searchStrs) {
            if (StringUtils.contains(str, searchStr))
                return true;
        }
        return ret;
    }

    public static void write2HDFS(String info, String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        OutputStream out = fs.create(new Path(path));
        StringReader reader;
        IOUtils.copyBytes(new StringBufferInputStream(info), out, conf);
    }
}
