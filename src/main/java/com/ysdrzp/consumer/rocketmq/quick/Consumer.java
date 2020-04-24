package com.ysdrzp.consumer.rocketmq.quick;

import com.ysdrzp.consumer.constants.RocketMQConstants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

public class Consumer {

    public static void main(String[] args) throws MQClientException {

        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quick_consumer_group");

        // Specify name server addresses.
        consumer.setNamesrvAddr(RocketMQConstants.NAME_SERVER_SINGLE);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // Subscribe one more more topics to consume.
        consumer.subscribe("quick_topic", "*");
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            try {
                System.out.println("----执行数据库操作----");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }catch (Exception e){
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }

}
