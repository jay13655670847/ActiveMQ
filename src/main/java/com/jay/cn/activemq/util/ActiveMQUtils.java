package com.jay.cn.activemq.util;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;

/**
 * @author lj
 * @version 1.0
 * @date 2021/9/25 17:50
 * ActiveMQ链接工具类
 */
public class ActiveMQUtils {

    public static final String ACTIVEMQ_IP="tcp://192.168.216.129:61616";

    private Connection connection;

    private Session session;

    @Before
    public void conn() throws JMSException {
        //获取工厂
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ACTIVEMQ_IP);
        //创建连接
        connection = factory.createConnection(ActiveMQConnectionFactory.DEFAULT_USER, ActiveMQConnectionFactory.DEFAULT_PASSWORD);
        //启动连接
        connection.start();
        session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * 获取消息生产者
     * @return
     * @throws JMSException
     */
    public MessageProducer getProducer() throws JMSException {
        Queue queue = session.createQueue("test");
        MessageProducer producer = session.createProducer(queue);

        return producer;
    }

    /**
     * 获取消息消费者
     * @return
     * @throws JMSException
     */
    public MessageConsumer getConsumer() throws JMSException {
        Destination queue = session.createQueue("test");
        MessageConsumer consumer = session.createConsumer(queue);

        return consumer;
    }

    @Test
    public void testProducer() throws JMSException, InterruptedException {
        MessageProducer producer = this.getProducer();
        for (int i = 0; i < 100; i++) {
            producer.send(session.createTextMessage("hello-"+i));
            Thread.sleep(1000);
        }
    }

    @Test
    public void testConsumer() throws JMSException {
        MessageConsumer consumer = this.getConsumer();
        while (true){
            Message message = consumer.receive();
            System.out.println(message);
        }

    }

    @After
    public void close() throws JMSException {
        connection.close();
    }
}
