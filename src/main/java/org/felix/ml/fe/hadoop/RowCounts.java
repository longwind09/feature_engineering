package org.felix.ml.fe.hadoop;

import org.apache.hadoop.mapreduce.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 *
 */
public class RowCounts {
    public static String COUNT_GROUP_NAME_MAPPER_PREFIX = "mapper_";
    public static String COUNT_GROUP_NAME_MAPPER_TOTAL = COUNT_GROUP_NAME_MAPPER_PREFIX + "total";
    public static String COUNT_GROUP_NAME_MAPPER_INFO = COUNT_GROUP_NAME_MAPPER_PREFIX + "info";
    public static String COUNT_GROUP_NAME_MAPPER_WARN = COUNT_GROUP_NAME_MAPPER_PREFIX + "warn";
    public static String COUNT_GROUP_NAME_MAPPER_ERROR = COUNT_GROUP_NAME_MAPPER_PREFIX + "error";

    public static String COUNT_GROUP_NAME_REDUCER_PREFIX = "reducer_";
    public static String COUNT_GROUP_NAME_REDUCER_TOTAL = COUNT_GROUP_NAME_REDUCER_PREFIX + "total";
    public static String COUNT_GROUP_NAME_REDUCER_INFO = COUNT_GROUP_NAME_REDUCER_PREFIX + "info";
    public static String COUNT_GROUP_NAME_REDUCER_WARN = COUNT_GROUP_NAME_REDUCER_PREFIX + "warn";
    public static String COUNT_GROUP_NAME_REDUCER_ERROR = COUNT_GROUP_NAME_REDUCER_PREFIX + "error";

    public static String COUNT_NAME_TOTAL = "total";
    public static String COUNT_NAME_PASS = "pass";
    public static String COUNT_NAME_FAIL = "fail";
    public static String COUNT_NAME_WARN = "warn";
    public static String COUNT_NAME_INFO = "info";
    private Map<String, Long> failCounts = new HashMap<String, Long>();
    private Map<String, Long> warnCounts = new HashMap<String, Long>();
    private Map<String, Long> infoCounts = new HashMap<String, Long>();
    private long totalRow = 0;
    private long passRow = 0;
    private long failRow = 0;
    private long warnRow = 0;
    private long infoRow = 0;

    public static RowCounts toRowCounts(Counters counters, boolean map) {
        RowCounts ret = new RowCounts();
        if (counters == null)
            return ret;
        CounterGroup cgroup = counters.getGroup(map ? COUNT_GROUP_NAME_MAPPER_TOTAL : COUNT_GROUP_NAME_REDUCER_TOTAL);
        if (cgroup == null)
            return ret;
        ret.totalRow = getCount(cgroup, COUNT_NAME_TOTAL, 0);
        ret.passRow = getCount(cgroup, COUNT_NAME_PASS, 0);
        ret.infoRow = getCount(cgroup, COUNT_NAME_INFO, 0);
        ret.warnRow = getCount(cgroup, COUNT_NAME_WARN, 0);
        ret.failRow = getCount(cgroup, COUNT_NAME_FAIL, 0);
        CounterGroup failGroup = counters
                .getGroup(map ? COUNT_GROUP_NAME_MAPPER_ERROR : COUNT_GROUP_NAME_REDUCER_ERROR);
        if (failGroup != null) {
            Iterator<Counter> failIter = failGroup.iterator();
            while (failIter.hasNext()) {
                Counter counter = failIter.next();
                if (counter == null)
                    continue;
                ret.failCounts.put(counter.getName(), counter.getValue());
            }
        }
        CounterGroup warnGroup = counters
                .getGroup(map ? COUNT_GROUP_NAME_MAPPER_WARN : COUNT_GROUP_NAME_REDUCER_WARN);
        if (warnGroup != null) {
            Iterator<Counter> warnIter = warnGroup.iterator();
            while (warnIter.hasNext()) {
                Counter counter = warnIter.next();
                if (counter == null)
                    continue;
                ret.warnCounts.put(counter.getName(), counter.getValue());
            }
        }
        CounterGroup infoGroup = counters
                .getGroup(map ? COUNT_GROUP_NAME_MAPPER_INFO : COUNT_GROUP_NAME_REDUCER_INFO);
        if (infoGroup != null) {
            Iterator<Counter> infoIter = infoGroup.iterator();
            while (infoIter.hasNext()) {
                Counter counter = infoIter.next();
                if (counter == null)
                    continue;
                ret.infoCounts.put(counter.getName(), counter.getValue());
            }
        }
        return ret;
    }

    public static long getCount(CounterGroup cgroup, String counterName, long defaultValue) {
        if (cgroup == null)
            return defaultValue;
        Counter counter = cgroup.findCounter(counterName);
        if (counter == null)
            return defaultValue;
        return counter.getValue();
    }

    public void incrementTotal() {
        totalRow++;
    }

    public void incrementPass() {
        passRow++;
    }

    public void incrementFail() {
        failRow++;
    }

    public void incrementWarn() {
        warnRow++;
    }

    public void incrementInfo() {
        infoRow++;
    }

    public void addFailCount(String field) {
        Long count = failCounts.get(field);
        if (count == null)
            failCounts.put(field, 1l);
        else
            failCounts.put(field, count.longValue() + 1);
        incrementFail();
    }

    public void addWranCount(String field) {
        Long count = warnCounts.get(field);
        if (count == null)
            warnCounts.put(field, 1l);
        else
            warnCounts.put(field, count.longValue() + 1);
        incrementWarn();
    }

    public void addInfoCount(String field) {
        Long count = infoCounts.get(field);
        if (count == null)
            infoCounts.put(field, 1l);
        else
            infoCounts.put(field, count.longValue() + 1);
        incrementInfo();
    }

    public void setMapperCounter(Mapper.Context context) {
        for (Map.Entry<String, Long> entry : failCounts.entrySet()) {
            Counter counter = context.getCounter(COUNT_GROUP_NAME_MAPPER_ERROR, entry.getKey());
            counter.increment(entry.getValue());
        }
        for (Map.Entry<String, Long> entry : warnCounts.entrySet()) {
            Counter counter = context.getCounter(COUNT_GROUP_NAME_MAPPER_WARN, entry.getKey());
            counter.increment(entry.getValue());
        }
        for (Map.Entry<String, Long> entry : infoCounts.entrySet()) {
            Counter counter = context.getCounter(COUNT_GROUP_NAME_MAPPER_INFO, entry.getKey());
            counter.increment(entry.getValue());
        }
        context.getCounter(COUNT_GROUP_NAME_MAPPER_TOTAL, COUNT_NAME_TOTAL).increment(totalRow);
        context.getCounter(COUNT_GROUP_NAME_MAPPER_TOTAL, COUNT_NAME_PASS).increment(passRow);
        context.getCounter(COUNT_GROUP_NAME_MAPPER_TOTAL, COUNT_NAME_FAIL).increment(failRow);
        context.getCounter(COUNT_GROUP_NAME_MAPPER_TOTAL, COUNT_NAME_WARN).increment(warnRow);
        context.getCounter(COUNT_GROUP_NAME_MAPPER_TOTAL, COUNT_NAME_INFO).increment(infoRow);
    }

    public void setReducerCounter(Reducer.Context context) {
        for (Map.Entry<String, Long> entry : failCounts.entrySet()) {
            Counter counter = context.getCounter(COUNT_GROUP_NAME_REDUCER_ERROR, entry.getKey());
            counter.increment(entry.getValue());
        }
        for (Map.Entry<String, Long> entry : warnCounts.entrySet()) {
            Counter counter = context.getCounter(COUNT_GROUP_NAME_REDUCER_WARN, entry.getKey());
            counter.increment(entry.getValue());
        }
        for (Map.Entry<String, Long> entry : infoCounts.entrySet()) {
            Counter counter = context.getCounter(COUNT_GROUP_NAME_REDUCER_INFO, entry.getKey());
            counter.increment(entry.getValue());
        }
        context.getCounter(COUNT_GROUP_NAME_REDUCER_TOTAL, COUNT_NAME_TOTAL).increment(totalRow);
        context.getCounter(COUNT_GROUP_NAME_REDUCER_TOTAL, COUNT_NAME_PASS).increment(passRow);
        context.getCounter(COUNT_GROUP_NAME_REDUCER_TOTAL, COUNT_NAME_FAIL).increment(failRow);
        context.getCounter(COUNT_GROUP_NAME_REDUCER_TOTAL, COUNT_NAME_WARN).increment(warnRow);
        context.getCounter(COUNT_GROUP_NAME_REDUCER_TOTAL, COUNT_NAME_INFO).increment(infoRow);
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("total:%s pass:%s info:%s warn:%s fail:%s pass_rate:%.2f fail_rate:%.2f\n", totalRow,
                passRow, infoRow, warnRow, failRow, totalRow == 0 ? -99f : 100 * passRow / totalRow,
                totalRow == 0 ? -99f : 100 * failRow / totalRow));
        if (failCounts.size() > 0) {
            sb.append("failNum\tfail/fail_total\tfailKey\n");
            for (Map.Entry<String, Long> entry : failCounts.entrySet()) {
                sb.append(String.format("%s\t%.2f\t%s\n", entry.getValue(),
                        failRow == 0 ? -99f : 100 * entry.getValue() / failRow, entry.getKey()));
            }
        }
        if (warnCounts.size() > 0) {
            sb.append("warnNum\twarn/warn_total\twarn/row_total\twarnKey\n");
            for (Map.Entry<String, Long> entry : warnCounts.entrySet()) {
                sb.append(String.format("%s\t%.2f\t%.2f\t%s\n", entry.getValue(),
                        warnRow == 0 ? -99f : 100 * entry.getValue() / warnRow,
                        warnRow == 0 ? -99f : 100 * entry.getValue() / totalRow, entry.getKey()));
            }
        }
        if (infoCounts.size() > 0) {
            sb.append("infoNum\tinfo/info_total\tinfo/row_total\tinfoKey\n");
            for (Map.Entry<String, Long> entry : infoCounts.entrySet()) {
                sb.append(String.format("%s\t%.2f\t%.2f\t%s\n", entry.getValue(),
                        infoRow == 0 ? -99f : 100 * entry.getValue() / infoRow,
                        infoRow == 0 ? -99f : 100 * entry.getValue() / totalRow, entry.getKey()));
            }
        }
        return sb.toString();
    }
}
