package com.freedom.activemq.api.consumer.selector;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class Producer {
    private ConnectionFactory connectionFactory;  //连接工厂
    private Connection connection; //连接
    private Session session;
    private MessageProducer messageProducer; //生产者

    public Producer(){
        try {
            this.connectionFactory = new ActiveMQConnectionFactory(
                    ActiveMQConnectionFactory.DEFAULT_USER,
                    ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                    "tcp://localhost:61616");
            this.connection = this.connectionFactory.createConnection();
            this.connection.start();
            this.session = this.connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            this.messageProducer = this.session.createProducer(null);
        }
        catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public Session getSession() {
        return this.session;
    }

    public void send(){
        try{
            Destination destination = this.session.createQueue("first");  //目的地

            //张三
            MapMessage mapMessage1 = this.session.createMapMessage();
            mapMessage1.setString("name", "张三");
            mapMessage1.setString("age", "23");
            mapMessage1.setStringProperty("color", "blue");
            mapMessage1.setIntProperty("sal", 2200);
            int id = 1;
            mapMessage1.setInt("id", id);
            String receiver = id%2 == 0 ? "A" : "B";
            mapMessage1.setStringProperty("receiver", receiver);

            //李四
            MapMessage mapMessage2 = this.session.createMapMessage();
            mapMessage2.setString("name", "李四");
            mapMessage2.setString("age", "26");
            mapMessage2.setStringProperty("color", "red");
            mapMessage2.setIntProperty("sal", 1300);
            id = 2;
            mapMessage2.setInt("id", id);
            receiver = id%2 == 0 ? "A" : "B";
            mapMessage2.setStringProperty("receiver", receiver);

            //王五
            MapMessage mapMessage3 = this.session.createMapMessage();
            mapMessage3.setString("name", "王五");
            mapMessage3.setString("age", "28");
            mapMessage3.setStringProperty("color", "green");
            mapMessage3.setIntProperty("sal", 1500);
            id = 3;
            mapMessage3.setInt("id", id);
            receiver = id%2 == 0 ? "A" : "B";
            mapMessage3.setStringProperty("receiver", receiver);

            //赵六
            MapMessage mapMessage4 = this.session.createMapMessage();
            mapMessage4.setString("name", "赵六");
            mapMessage4.setString("age", "30");
            mapMessage4.setStringProperty("color", "blue");
            mapMessage4.setIntProperty("sal", 1800);
            id = 4;
            mapMessage4.setInt("id", id);
            receiver = id%2 == 0 ? "A" : "B";
            mapMessage4.setStringProperty("receiver", receiver);

            //发送消息
            this.messageProducer.send(destination, mapMessage1, DeliveryMode.NON_PERSISTENT, 2, 1000*60*10);
            this.messageProducer.send(destination, mapMessage2, DeliveryMode.NON_PERSISTENT, 3, 1000*60*10);
            this.messageProducer.send(destination, mapMessage3, DeliveryMode.NON_PERSISTENT, 6, 1000*60*10);
            this.messageProducer.send(destination, mapMessage4, DeliveryMode.NON_PERSISTENT, 9, 1000*60*10);
        }
        catch (JMSException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        Producer producer = new Producer();
        producer.send();
    }
}
