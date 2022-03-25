package com.pn;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HeaderExchangeChannel {
    private String exchangeName;
    private Channel channel;
    private Connection connection;

    public HeaderExchangeChannel(Connection connection, String exchangeName) throws IOException {
        this.connection = connection;
        this.exchangeName = exchangeName;
        this.channel = connection.createChannel();
    }

    public void declareExchange() throws IOException {
        //exchangeDeclare(exchange, builtinExchangeType, durable)
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.HEADERS, true);
    }

    public void declareQueues(String ...queuesName) throws IOException {
        for(String queue: queuesName){
            //queueDeclare ( queueName, durable, exclusive, autoDelete, arguments)
            channel.queueDeclare(queue, true, false, false, null);
        }
    }

    public void performQueueBinding(String queueName, Map<String, Object> headers) throws IOException {
        //create bindings - (queue, exchange, routingKey, headers)
        channel.queueBind(queueName, exchangeName, "", headers);
    }

//    public void subscribeMessage(String queueName) throws IOException {
//        //basicConsume - (queue, autoAck, deliverCallback, cancelCallback
//        DeliverCallback deliverCallback = (consumerTag, messsage) ->{
//            System.out.println("[received] [" + queueName+"]: " + consumerTag);
//            System.out.println("[received] [" + queueName+"]: " + new String(messsage.getBody()));
//        };
//        CancelCallback cancelCallback = System.out::println;
//        channel.basicConsume(exchangeName, true, deliverCallback, cancelCallback);
//    }

    public void subscribeMessage(String queueName) throws IOException {
        // basicConsume - ( queue, autoAck, deliverCallback, cancelCallback)
        channel.basicConsume(queueName, true, ((consumerTag, message) -> {
            System.out.println("[Received] [" + queueName + "]: " + consumerTag);
            System.out.println("[Received] [" + queueName + "]: " + new String(message.getBody()));
        }), consumerTag -> {
            System.out.println(consumerTag);
        });
    }

    public void publishMessage(String message, Map<String, Object> headers) throws IOException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties()
                                            .builder().headers(headers).build();
        //basicPublish - (exchange, routingkey, basicProperties, body)
        channel.basicPublish(exchangeName,"", properties,
                            message.getBytes(StandardCharsets.UTF_8));
    }


}
