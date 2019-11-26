package com.redhat.demo;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        // System.out.println("Hello World!");
        // mvn exec:java -Dexec.mainClass="com.redhat.demo.App"
        // -D broker.url="failover://(amqp://192.168.0.110:5672)" -Dsend.queue=queue1
        // -Dsend.msg=helloworld! -Dsend.mode=SEND/RECV
        App client1 = new App(System.getProperty("broker.url"));
        // SEND or RECV
        String mode = System.getProperty("send.mode");

        client1.connect(mode);
    }

    Logger log = LoggerFactory.getLogger(this.getClass());
    private String brokerUrl;

    public App(String brokerUrl) {
        this.brokerUrl = brokerUrl;
        log.info("connecting to " + brokerUrl);
    }

    private void simpleSend(Session session, TextMessage message, Queue queue) throws JMSException {
        MessageProducer sender = session.createProducer(queue);
        sender.setDeliveryMode(DeliveryMode.PERSISTENT);
        log.info("Sending "+message+" to "+queue.getQueueName());
        sender.send(message);
        //sender.send(session.createTextMessage("END"));
        //log.info("Sent msg END");
    }

    private void simpleSend(Session session, String message, Queue queue) throws JMSException {
        this.simpleSend(session, session.createTextMessage(message), queue);
    }

    private void send(Session session, String message, Queue queue) throws JMSException {
        MessageProducer sender = session.createProducer(queue);
        sender.setDeliveryMode(DeliveryMode.PERSISTENT);
        // for (int i = 0; i < 3; i++) {
        int i = 0;
        
        while (true) {
            try {
                // sender.send(session.createTextMessage(message));
                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSSS");
                Date dt = new Date();
                
                String text = message +"-"+sdf.format(dt)+ "-"+(i++);
                sender.send(session.createTextMessage(text));
                log.info("Sent msg " + i + ": " + text);
                //log.info("sleep for 10 ms");
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if (i == 5000) {
                break;
            }

        }
        log.info("sent total "+(i+1));
        //sender.send(session.createTextMessage("END"));
        //log.info("Sent msg END");
    }

    private void receive(Session session, Queue queue) throws JMSException {
        MessageConsumer consumer = session.createConsumer(queue);
        int recvCount = 0;
        while (true) {
            TextMessage m = (TextMessage) consumer.receive(0);
            log.info(m.getText());
            if ("END".equals(m.getText())) {
                log.info("END");
                break;
            }

            String reply=System.getProperty("reply");
            log.info("reply mode:"+reply);
            if (m != null) {
                log.info("received message # "+(++recvCount)+" :"+m.getText() + " "+m.getJMSDestination().toString());
                if ("TRUE".equals(reply)) {
                    //did the message has a replyto?
                    //intro random delay

                    Queue replyQueue=(Queue)m.getJMSReplyTo();
                    log.info("got reply queue:"+replyQueue.getQueueName());
                    log.info("got corelationId:"+m.getJMSCorrelationID());
                    
                    TextMessage replyMessage = session.createTextMessage("REPLY");
                    replyMessage.setJMSCorrelationID(m.getJMSCorrelationID());
                    //call reply
                    simpleSend(session, replyMessage, replyQueue);
                }    
                //m.acknowledge();
                log.info("total recv "+recvCount);

            }
        }//true
    }

    private void requestReply(Session session, String message, Queue queue) throws JMSException {
        Queue replyQueue = session.createQueue(System.getProperty("reply.queue"));
        String clientId = System.getProperty("clientId");
        MessageProducer sender = session.createProducer(queue);
        sender.setDeliveryMode(DeliveryMode.PERSISTENT);
        TextMessage textMessage=session.createTextMessage(message);
        textMessage.setJMSReplyTo(replyQueue);
        textMessage.setJMSCorrelationID(clientId+":"+System.currentTimeMillis());
        sender.send(textMessage);
        log.info("Sent msg : " + textMessage.getText()+" / "+textMessage.getJMSCorrelationID());
        
        
        //wait for reply
        


        MessageConsumer consumer = session.createConsumer(replyQueue);
        log.info("wait for reply : "+replyQueue.getQueueName() );
        
        while (true) {

            TextMessage m = (TextMessage) consumer.receive(1000);

            if (m!=null && m.getText().equals("REPLY")) {
               log.info("replied from remote client");
               log.info("info "+m.getText()+"/"+m.getJMSCorrelationID());
               break;
            } 
        }

        //sender.send(session.createTextMessage("END"));
        //log.info("Sent msg END");
        log.info("OK!" );

    }

    private void connect(String mode) {
        Connection connection = null;

        ConnectionFactory connectionFactory = new JmsConnectionFactory("admin","admin",this.brokerUrl);

        try {
            connection = connectionFactory.createConnection();
            //Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session session = null;
            Queue queue = null;
            //String mode = System.getProperty("send.mode");
            if ("SEND".equals(mode)) {
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                queue = session.createQueue(System.getProperty("send.queue"));
                String message=System.getProperty("send.msg");
                send(session, message, queue);
            }
            else if ("RECV".equals(mode)) {
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                queue = session.createQueue(System.getProperty("send.queue"));
                connection.start();
                receive(session, queue);
            }
            else if ("REPLY".equals(mode)) {
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                queue = session.createQueue(System.getProperty("send.queue"));
                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSSS");
                Date dt = new Date();
                String message=System.getProperty("send.msg")+"-"+sdf.format(dt);
                connection.start();
                requestReply(session, message, queue);
            }
            else if ("SIMPLESEND".equals(mode)) {
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                queue = session.createQueue(System.getProperty("send.queue"));
                String message=System.getProperty("send.msg");
                simpleSend(session, message, queue);
            }

            //all done
            session.close();
            
        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            if (connection != null) {
               // Step 9. close the connection
                try {
                    connection.close();
                } catch (JMSException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
         }

    }
}
