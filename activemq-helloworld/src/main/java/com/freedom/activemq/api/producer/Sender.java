package com.freedom.activemq.api.producer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;


public class Sender {
    public static void main(String[] args) throws Exception {
        //第一步：建立ConnectionFactory工厂对象，需要填入用户名、密码、以及要连接的地址，均使用默认即可，默认端口号为"tcp://localhost:61616"
        ConnectionFactory ConnectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,      //默认用户名，密码为null
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
        Destination destination = session.createQueue("queue1"); //如果queue1已经存在，只会往queue1中添加消息

        //第五步：通过Session创建生产者或消费者(MessageProducer/MessageConsumer)，通过参数指定Destination目的地
        MessageProducer producer = session.createProducer(null);  //不指定destination

        //第六步：可以使用MessageProducer的setDeliveryMode()方法为其设置持久化特性和非持久化特性（DeliveryMode）
        //producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); //可以针对单个生产者设置是否持久化
                                                                 //非持久化在MQ重启后消息丢失，但queue1队列还存在

        //第七步：最后通过Session创建JMS规范中TextMessage形式的数据，并用MessageProducer的send()方法发送数据
        //        同理，使用receive()方法接收数据
        //        最后不要忘记关闭Connection连接
        for(int i=1; i<=5; i++){
            TextMessage textMessage = session.createTextMessage();
            textMessage.setText("我是消息内容，id为：" + i);

            producer.send(destination,                    //第一个参数：目的地
                          textMessage,                    //第二个参数：消息
                          DeliveryMode.NON_PERSISTENT,  //第三个参数：是否持久化
                          i,                              //第四个参数：优先级（0-9十个级别，0-4是普通级别，5-9是加急，默认是4）
                          1000 * 60 * 2);                 //第五个参数：消息在MQ上的存放有效期（超过有效期，控制台看，消息不是消失了，而是和被消费了一样）

            System.out.println("生产者：" + textMessage.getText());
        }

        if(connection != null){
            connection.close(); //connection.close()内部会关闭session等
        }
    }
}
