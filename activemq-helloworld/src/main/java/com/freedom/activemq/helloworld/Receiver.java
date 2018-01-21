package com.freedom.activemq.helloworld;

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
                Boolean.FALSE,
                Session.AUTO_ACKNOWLEDGE);

        //第四步：通过Session创建Destination对象，指的是客户端用来指定生产消息目标或消费消息来源的对象
        //        在PTP模式中，Destination被称作Queue即队列；在Pub/Sub模式中，Destination被称作Topic即主题
        //        在程序中可以使用多个Queue或Topic
        Destination destination = session.createQueue("queue1"); //如果queue1已经存在，只是创建到queue1的Destination

        //第五步：通过Session创建生产者或消费者(MessageProducer/MessageConsumer)，通过参数指定Destination目的地
        MessageConsumer consumer = session.createConsumer(destination);

        //第六步：消费者不需要设置是否持久化
        //producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        //第七步：循环消费消息
        while (true){
            TextMessage msg = (TextMessage)consumer.receive();  //阻塞的接收消息
            if(msg == null) break;
            System.out.println("收到的内容：" + msg.getText());
        }

        if(connection != null){
            connection.close(); //connection.close()内部会关闭session等
        }
    }
}
