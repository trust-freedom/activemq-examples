package com.freedom.activemq.pubsub;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Subscriber2 {
    private ConnectionFactory connectionFactory;  //连接工厂
    private Connection connection;  //连接对象
    private Session session;
    private MessageConsumer consumer;  //消费者

    public Subscriber2(){
        try {
            this.connectionFactory = new ActiveMQConnectionFactory(
                    ActiveMQConnectionFactory.DEFAULT_USER,
                    ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                    "tcp://localhost:61616");
            this.connection = this.connectionFactory.createConnection();
            this.connection.start();
            this.session = this.connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
        }
        catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void receive() throws Exception{
        Destination destination = session.createTopic("topic1");
        consumer = session.createConsumer(destination);
        consumer.setMessageListener(new Listener());
    }

    class Listener implements MessageListener {
        @Override
        public void onMessage(Message message) {
            try {
                if(message instanceof TextMessage){
                    TextMessage textMessage = (TextMessage)message;
                    System.out.println("Subscriber2: " + textMessage.toString());
                    System.out.println("Subscriber2: " + textMessage.getText());
                }

            }
            catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Subscriber2 subscriber = new Subscriber2();
        subscriber.receive();
    }
}
