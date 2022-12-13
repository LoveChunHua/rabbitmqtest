package rabbitmq.three;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import rabbitmq.utils.RabbitMqUtils;
import rabbitmq.utils.SleepUtils;

public class Worker03 {
    private static final String ACK_QUEUE_NAME = "ack_queue";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        //不公平分发 PrefetchCount=1；其他值时是预先分配分发数量
        int PrefetchCount = 5;
        channel.basicQos(PrefetchCount);
        System.out.println("C3 等待接收消息处理时间较短");
        //消息消费的时候如何处理消息
        DeliverCallback deliverCallback = (consumerTag, delivery) ->
        {
            String message = new String(delivery.getBody());
            SleepUtils.sleep(1);
            System.out.println("接收到消息:" + message);
            /**
             * 1.消息标记 tag
             * 2.是否批量应答未应答消息
             */
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        //采用手动应答
        boolean autoAck = false;
        channel.basicConsume(ACK_QUEUE_NAME, autoAck, deliverCallback, (consumerTag) -> {
            System.out.println(consumerTag + "消费者取消消费接口回调逻辑");
        });
    }
}
