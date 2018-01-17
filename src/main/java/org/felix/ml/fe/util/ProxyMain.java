package org.felix.ml.fe.util;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * 因为要执行的主函数比较多，离散化、onehot编码、重新组织样本，共三步,有一个共同的程序入口
 * 其实主要原因是当前hadoop jar 提交任务时难以指定主函数，所以多个hadoop 任务在一个jar包里需要一个统一入口
 * 这个统一入口需要在pom文件里指定
 */
public class ProxyMain {
    private static Logger info = Logger.getLogger("info");
    private static Logger warn = Logger.getLogger("warn");

    /**
     * 识别参数中的主类，用class.forName 感觉不太好
     *
     * @param args
     * @return
     */
    public static int findMain(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (StringUtils.startsWith(arg, "-D")) {
                continue;
            }
            try {
                Class main = Class.forName(arg);
                return i;
            } catch (ClassNotFoundException e) {
                continue;
            }
        }
        return -1;
    }

    /**
     * 去掉主类，留下其他的参数
     *
     * @param args
     * @param id
     * @return
     */
    public static String[] delMain(String[] args, int id) {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < args.length; i++) {
            if (i == id)
                continue;
            if (StringUtils.isEmpty(args[i]))
                continue;
            list.add(args[i]);
        }
        return list.toArray(new String[]{});
    }

    /**
     * 主函数入口，参数是真正的入口类，以及对应的参数
     *
     * @param args
     * @throws ClassNotFoundException    主类
     * @throws NoSuchMethodException     主函数
     * @throws SecurityException
     * @throws IllegalAccessException    函数反射
     * @throws IllegalArgumentException  函数
     * @throws InvocationTargetException 函数
     */
    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, SecurityException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // java的参数不带程序自身
        if (args.length < 1) {
            warn.error("usage:");
            warn.error("proxyMain mainClass args...");
            System.exit(-1);
        }
        //这个log4j配置
        PropertyConfigurator.configure(ProxyMain.class.getResourceAsStream("/sample_log4j.properties"));
        //识别主类
        int id = findMain(args);
        if (id < 0) {
            warn.error("can't find main!!");
            System.exit(-1);
        }
        String className = args[id];
        //这个做了多遍
        Class main = Class.forName(className);

        //这个没有用？？
        Class[] parameterTypes = new Class[1];
        //反射获得主函数
        Method mainMethod = main.getMethod("main", String[].class);
        //识别参数
        String[] nargs = delMain(args, id);
        info.info("============start " + className + " process===============");
        info.info("run ProxyMain, args: {" + StringUtils.join(args, " ") + "}");

        //运行主函数
        try {
            mainMethod.invoke(null, new Object[]{nargs});
        } catch (Throwable e) {
            info.info("============fail " + className + " process===============");
            warn.error(String.format("ERROR process classname:%s, args:{%s}", className, StringUtils.join(nargs, " ")), e);
            System.exit(-1);
        }
        info.info("============end " + className + " process===============");
    }
}
