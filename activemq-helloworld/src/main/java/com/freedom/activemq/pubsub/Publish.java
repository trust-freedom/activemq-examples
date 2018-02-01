package com.freedom.activemq.pubsub;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Publish {
    private ConnectionFactory connectionFactory;  //连接工厂
    private Connection connection;  //连接对象
    private Session session;
    private MessageProducer producer;  //生产者

    public Publish(){
        try {
            this.connectionFactory = new ActiveMQConnectionFactory(
                    ActiveMQConnectionFactory.DEFAULT_USER,
                    ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                    "tcp://localhost:61616");
            this.connection = this.connectionFactory.createConnection();
            this.connection.start();
            this.session = this.connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            this.producer = this.session.createProducer(null);
        }
        catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发布消息
     */
    public void sendMessage() throws Exception{
        Destination destination = session.createTopic("topic1");
        TextMessage textMessage = session.createTextMessage("我是消息内容");
        producer.send(destination, textMessage);
    }

    public static void main(String[] args) throws Exception{
        Publish publish = new Publish();
        publish.sendMessage();
    }
}
