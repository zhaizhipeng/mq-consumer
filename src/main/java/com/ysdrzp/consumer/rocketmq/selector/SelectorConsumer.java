package com.ysdrzp.consumer.rocketmq.selector;

import com.ysdrzp.consumer.constants.RocketMQConstants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消息过滤
 */
public class SelectorConsumer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketMQConstants.CONSUMER_GROUP);

        consumer.setNamesrvAddr(RocketMQConstants.NAME_SERVER_SINGLE);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //consumer.subscribe("selector_topic", "TagA || TagB || TagC");

        consumer.subscribe("selector_topic", MessageSelector.bySql("orderId > 10000l"));

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                try {
                    for (MessageExt messageExt : list){
                        System.out.println("messageQueue:" + consumeOrderlyContext.getMessageQueue());
                        System.out.println("commitLogOffset:" + messageExt.getCommitLogOffset() + ",tags:" + messageExt.getTags() + ",keys:" + messageExt.getKeys() + ",body:" + new String(messageExt.getBody()));
                    }
                }catch (Exception e){
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
