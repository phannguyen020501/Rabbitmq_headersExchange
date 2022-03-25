package com.pn;

import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private static HeaderExchangeChannel channel;

    public void start() throws IOException, TimeoutException {
        //create connection
        Connection connection = ConnectionManager.createConnection();

        //create channel
        channel = new HeaderExchangeChannel(connection,Constant.EXCHANGE_NAME);

        //creat header exchange
        channel.declareExchange();

        //create headers
        Map<String, Object> devHeader = new HashMap<>();
        devHeader.put("x-match","any");//match any of the header
        devHeader.put("dev", "Developer Channel");
        devHeader.put("general", "General Channel");

        Map<String, Object> managerHeader = new HashMap<>();
        managerHeader.put("x-match", "any");//match any of the header
        managerHeader.put("dev", "Developer Channel");
        managerHeader.put("manager", "Manager Channel");
        managerHeader.put("general", "General Channel");

        Map<String, Object> publishedHeader = new HashMap<>();
        publishedHeader.put("x-match", "all");//match all the header
        publishedHeader.put("general", "General Channel");
        publishedHeader.put("access", "publish");

        //create queues
        channel.declareQueues(Constant.DEV_QUEUE_NAME,
                Constant.MANAGER_QUEUE_NAME, Constant.PUBLISHED_QUEUE_NAME);

        //binding queues with headers
        channel.performQueueBinding(Constant.DEV_QUEUE_NAME, devHeader);
        channel.performQueueBinding(Constant.MANAGER_QUEUE_NAME, devHeader);
        channel.performQueueBinding(Constant.PUBLISHED_QUEUE_NAME, publishedHeader);
    }

    //subscribe message
    public void subscribe() throws IOException {
        //subscribe message
        channel.subscribeMessage(Constant.DEV_QUEUE_NAME);
        channel.subscribeMessage(Constant.MANAGER_QUEUE_NAME);
        channel.subscribeMessage(Constant.PUBLISHED_QUEUE_NAME);

    }
}
