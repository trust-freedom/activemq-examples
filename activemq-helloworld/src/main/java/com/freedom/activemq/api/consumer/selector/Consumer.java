package com.freedom.activemq.api.consumer.selector;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Consumer {
    private ConnectionFactory connectionFactory;  //连接工厂
    private Connection connection;  //连接
    private Session session;
    private MessageConsumer messageConsumer;  //消费者
    private Destination destination;  //目标地址

    public final String SELECTOR_1 = "color = 'blue'";
    public final String SELECTOR_2 = "color = 'blue' AND sal > 2000";
    public final String SELECTOR_3 = "receiver = 'A'";

    public Consumer(){
        try {
            this.connectionFactory = new ActiveMQConnectionFactory(
                    ActiveMQConnectionFactory.DEFAULT_USER,
                    ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                    "tcp://localhost:61616");
            this.connection = this.connectionFactory.createConnection();
            this.connection.start();
            this.session = this.connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            this.destination = this.session.createQueue("first");
            this.messageConsumer = this.session.createConsumer(this.destination, SELECTOR_2);
        }
        catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void receiver(){
        try {
            this.messageConsumer.setMessageListener(new Listener());
        }
        catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * 监听类
     */
    class Listener implements MessageListener {
        @Override
        public void onMessage(Message message) {
            try {
                if(message instanceof TextMessage){

                }
                else if(message instanceof MapMessage){
                    MapMessage mapMessage = (MapMessage)message;
                    System.out.println(mapMessage.toString());
                    System.out.println(mapMessage.getString("name"));
                    System.out.println(mapMessage.getString("age"));

                }
            }
            catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args){
        Consumer consumer = new Consumer();
        consumer.receiver();
    }
}
