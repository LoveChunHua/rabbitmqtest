package rabbitmq.four;

import com.rabbitmq.client.Channel;
import rabbitmq.utils.RabbitMqUtils;

import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConfirmMessage {
    public static void main(String[] args) throws Exception {
//        ConfirmMessage.publishIndividually();
        ConfirmMessage.publishBatch();
        ConfirmMessage.publishAsync();
    }

    //单个确认
    public static void publishIndividually() throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName, true, false, false, null);
        channel.confirmSelect();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            String msg = i + "";
            channel.basicPublish("", queueName, null, msg.getBytes());
            boolean flag = channel.waitForConfirms();
            if (flag) {
                System.out.println("消息发送成功");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("发布" + "1000个单独确认消息，耗时" + (end - begin) + "ms");
    }

    //批量确认
    public static void publishBatch() throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName, true, false, false, null);
        channel.confirmSelect();
        long begin = System.currentTimeMillis();
        //批量确认的长度
        int batchSize = 100;
        for (int i = 0; i < 1000; i++) {
            String msg = i + "";
            channel.basicPublish("", queueName, null, msg.getBytes());
            if (i % batchSize == 0) {
                //发布确认
                channel.waitForConfirms();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("发布" + "1000个批量确认消息，耗时" + (end - begin) + "ms");
    }

    //异步确认
    public static void publishAsync() throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName, true, false, false, null);
        channel.confirmSelect();

        /**
         * 线程安全的的哈希表 适用于高并发情况 ，发布消息的是一个线程，监听器是一个线程
         */
        ConcurrentSkipListMap<Long, String> map = new ConcurrentSkipListMap();


        //消息监听器 监听哪些消息成功 哪些消息失败
        /**
         * deliveryTag 消息的编号
         * multiple 是否为批量确认
         */
        channel.addConfirmListener((deliveryTag, multiple) -> {
            //删除已经确认的消息
            if (multiple) {
                ConcurrentNavigableMap<Long, String> confirmed = map.headMap(deliveryTag);
                confirmed.clear();
            } else {
                map.remove(deliveryTag);
            }

            System.out.println("确认的消息" + deliveryTag);
        }, (deliveryTag, multiple) -> {
            //打印未确认的消息
            System.out.println("未确认的消息" + deliveryTag);
        });
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            String msg = i + "";
            channel.basicPublish("", queueName, null, msg.getBytes());
            //记录所有要发送的消息
            map.put(channel.getNextPublishSeqNo(), msg);
        }
        long end = System.currentTimeMillis();
        System.out.println("发布" + "1000个异步确认消息，耗时" + (end - begin) + "ms");
    }
}
