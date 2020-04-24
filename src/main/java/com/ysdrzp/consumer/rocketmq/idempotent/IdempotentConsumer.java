package com.ysdrzp.consumer.rocketmq.idempotent;

import com.alibaba.fastjson.JSON;
import com.ysdrzp.consumer.constants.RocketMQConstants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 幂等消费
 */
public class IdempotentConsumer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("mq_consumer_group");

        consumer.setNamesrvAddr(RocketMQConstants.NAME_SERVER_SINGLE);

        consumer.subscribe("order_pay_success_topic", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println(list);
                System.out.println(consumeConcurrentlyContext);
                try {
                    //
                    for (MessageExt messageExt : list){
                        System.out.println("keys:" + messageExt.getKeys());
                        if (decide(messageExt.getKeys())){
                            // 模拟数据库异常
                            int i = 1 / 0;
                            System.out.println("----操作数据库----");
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }catch (Exception e){
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }

            private boolean decide(String key){
                if ("PO20200424150000000".endsWith(key)){
                    return true;
                }
                return false;
            }
        });

        consumer.start();

        System.out.println("----consumer start----");
    }

}
