package com.freedom.activemq.api.producer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;


public class Receiver {
    public static void main(String[] args) throws Exception {
        //第一步：建立ConnectionFactory工厂对象，需要填入用户名、密码、以及要连接的地址，均使用默认即可，默认端口号为"tcp://localhost:61616"
        ConnectionFactory ConnectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://localhost:61616");

        //第二步：通过ConnectionFactory工厂建立Connection连接，并且调用Connection的start()方法开启连接，Connection默认是关闭的
        Connection connection = ConnectionFactory.createConnection();
        connection.start();

        //第三步：通过Connection对象创建Session会话（上下文环境对象），用于发送/接收消息
        //       参数1 - 是否启用事务
        //       参数2 - 签收模式，一般设置自动签收
        Session session = connection.createSession(
                Boolean.FALSE,                      //是否开启事务
                Session.CLIENT_ACKNOWLEDGE);      //客户端签收模式

        //第四步：通过Session创建Destination对象，指的是客户端用来指定生产消息目标或消费消息来源的对象
        //        在PTP模式中，Destination被称作Queue即队列；在Pub/Sub模式中，Destination被称作Topic即主题
        //        在程序中可以使用多个Queue或Topic
        Destination destination = session.createQueue("queue1"); //如果queue1已经存在，只是创建到queue1的Destination

        //第五步：通过Session创建生产者或消费者(MessageProducer/MessageConsumer)，通过参数指定Destination目的地
        MessageConsumer consumer = session.createConsumer(destination);

        //第六步：消费者不需要设置是否持久化
        //producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        /**
         * 第七步：循环消费消息
         * 收到消息的顺序还是：
         *   收到的内容：我是消息内容，id为：1
         *   收到的内容：我是消息内容，id为：2
         *   收到的内容：我是消息内容，id为：3
         *   收到的内容：我是消息内容，id为：4
         *   收到的内容：我是消息内容，id为：5
         * 而按照生产者对消息优先级的设置，第5条消息的优先级应该最高，而却没有第一个收到
         * 即使生产者启用事务，即5条消息是在sesssion.commit()时统一由生产者发送的，但收到的还是如上
         * 所以优先级只是理论上有用，不是绝对的
         */
        while (true){
            TextMessage msg = (TextMessage)consumer.receive();  //阻塞的接收消息
                                                                //如果是自动签收，receive()执行完就已经签收了
            msg.acknowledge();  //手工签收消息，另起一个线程（TCP通讯线程）去通知MQ服务，确认签收
                                //没有签收也能收到消息，但MQ的queue1队列的pending数量没有减少，dequeued数量没有增加，也就是MQ不认为消息被消费了
            if(msg == null) break;
            System.out.println("收到的内容：" + msg.getText());
        }

        if(connection != null){
            connection.close(); //connection.close()内部会关闭session等
        }
    }
}
