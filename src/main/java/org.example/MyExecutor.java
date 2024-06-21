package org.example;

import static java.lang.Thread.sleep;
import static java.util.stream.Collectors.joining;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author qianyujun <qianyujun@kuaishou.com>
 * Created on 2024-06-20
 */
public class MyExecutor implements Callable<Integer> {
    private long uid;
    private int index;
    private String query;
    private final String endPoint = "https://kwaipro-candidate.test.gifshow.com/rest/o/search/ai/status/seo?";
    private final int timeOut = 4000;
    private static final int QUEUE_SIZE = 30;

    final Logger logger = LogManager.getLogger(MyExecutor.class);

    public MyExecutor(String query, long uid, int index) {
        this.query = query;
        this.uid = uid;
        this.index = index;
    }

    private String encodeValue(String value)  {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            return "";
        }
    }

    @Override
    public Integer call() throws Exception {
        Map<String, String> requestParams = new HashMap<>();
        requestParams.put("keyword", query);
        requestParams.put("generateVideo", "true");
        requestParams.put("publisherId", String.valueOf(uid));
        requestParams.put("index", String.valueOf(index));

        String encodedURL = requestParams.keySet().stream()
                .map(key -> key + "=" + encodeValue(requestParams.get(key)))
                .collect(joining("&", endPoint, ""));

        logger.info("encodedURL is: {}", encodedURL);

        LocalTime now = LocalTime.now();
        int seconds = now.getSecond();
        int expectedSeconds = seconds % QUEUE_SIZE;

        while (expectedSeconds != index) {
            sleep(1000);
            now = LocalTime.now();
            seconds = now.getSecond();
            expectedSeconds = seconds % QUEUE_SIZE;
        }

        URL obj = new URL(encodedURL);
        HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setConnectTimeout(timeOut);

        String body = "{}";

        connection.setDoOutput(true);
        OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
        writer.write(body);
        writer.flush();
        writer.close();

        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line;

        StringBuffer response = new StringBuffer();

        while ((line = br.readLine()) != null) {
            response.append(line);
        }
        br.close();

        return 0;
    }
}
