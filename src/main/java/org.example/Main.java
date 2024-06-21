package org.example;

import static java.lang.Thread.sleep;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author qianyujun <qianyujun@kuaishou.com>
 * Created on 2024-06-17
 */
public class Main {
    private static final int QUEUE_SIZE = 30;
    private static final int QUERY_REPEAT = 5;

    public static void main(String[] args) throws InterruptedException {
        final Logger logger = LogManager.getLogger(Main.class);
        if (args.length != 2) {
            logger.info("arg length is: {}", args.length);
            logger.info("usage: java org.example.Main queryFile uidFile");
            return;
        }
        String queryFile = args[0];
        String uidFile = args[1];

        List<String> queryList = new ArrayList<>();
        List<Long> uidList = new ArrayList<>();

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(queryFile));
            String line = reader.readLine();

            while (line != null) {
                queryList.add(line);
                line = reader.readLine();
            }
            reader.close();

            reader = new BufferedReader(new FileReader(uidFile));
            line = reader.readLine();

            while (line != null) {
                uidList.add(Long.valueOf(line));
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        long i = 0;
        int j = 0;
        int k = 0;
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(QUEUE_SIZE);;
        while (j < queryList.size()) {
            int uidIndex = (int) (i % uidList.size());
            int index = (int) (i % QUEUE_SIZE);
            executor.submit(new MyExecutor(queryList.get(j), uidList.get(uidIndex), index));
            logger.info("query is: {}", queryList.get(j));
            k++;
            if (k >= QUERY_REPEAT) {
                k = 0;
                j++;
            }
            i++;
            logger.info("thread pool size is: {}", executor.getPoolSize());
            logger.info("thread pool queue size is: {}", executor.getQueue().size());
            sleep(1000);
        }
        executor.shutdown();
        executor.awaitTermination(3000, TimeUnit.SECONDS);
    }
}
