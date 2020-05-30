package com.redhat.demo.drainer;

import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.protocol.core.impl.CoreProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DrainerAMQ
 */
public class DrainerAMQ {

    public static void main(String[] args) {

        DrainerAMQ drainer = new DrainerAMQ();
        drainer.drain();
        
    }// main

    private ActiveMQServer server;
    SecurityConfiguration securityConfig;
    ActiveMQJAASSecurityManager securityManager;
    Logger log = LoggerFactory.getLogger(this.getClass());
    String[] queueNames;
    ConnectionFactory sourceConnectionFactory, targetConnectionFactory;
    Connection sourceConnection, targetConnection;
    Session sourceSession, targetSession;

    public DrainerAMQ() {
        log.info("constructor");
        this.init();
    }

    private void init() {
        log.info("init");

        securityConfig = new SecurityConfiguration();
        securityConfig.addUser("admin", "admin");
        securityConfig.addRole("admin", "amq");
        securityConfig.setDefaultUser("admin");
        securityManager = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), securityConfig);
        try {
            log.info("creating embedded server pointing to source broker");

            server = ActiveMQServers.newActiveMQServer("broker.xml", null, securityManager);
            server.start();
            queueNames = server.getActiveMQServerControl().getQueueNames();
            log.info("started server");
            log.info("found these queues");

            for (String queueName : queueNames) {
                log.info(queueName);
            }
            initConnections();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void initConnections() {
        // init source
        sourceConnectionFactory = new ActiveMQConnectionFactory("vm://0");
        //init target connection
       targetConnectionFactory = new ActiveMQConnectionFactory("tcp://dev:61616");


        try {
            sourceConnection = sourceConnectionFactory.createConnection("admin", "admin");
            sourceSession = sourceConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            targetConnection = targetConnectionFactory.createConnection("admin", "admin");
            targetSession = targetConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    public void drain() {

        try {
            for (String queueName : queueNames) {
                //hack
                //queueName = "queue2";
                log.info("----"+queueName+"----");
                Queue sourceQueue=sourceSession.createQueue(queueName);
                Queue targetQueue=targetSession.createQueue(queueName);
                MessageConsumer messageConsumer = sourceSession.createConsumer(sourceQueue);
                sourceConnection.start();
                long start = System.currentTimeMillis();
                log.info("start now "+start);
                boolean endLoop = false;
                while (endLoop == false) {
                    
                    TextMessage messageReceived = (TextMessage) messageConsumer.receive(500);
                    

                    if (messageReceived != null) {
                        log.info(">>>>>>>Received message:" + messageReceived.getText());
                        log.info("sending to target");
                        MessageProducer producer = targetSession.createProducer(targetQueue);
                        producer.send(messageReceived);
                    }
                    //log.info(System.currentTimeMillis() - start +"");
                    if (System.currentTimeMillis() - start > 5000 ){
                        log.info("i am moving on....no more messages after 5 sec");
                        endLoop = true;
                    }
                } //
                //log.info("exited while");                
                
            } //for           
              //log.info("exited for");                
        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            try {
                
            if (sourceConnection!=null ) {
                sourceConnection.close();
            }
            if (targetConnection!=null ) {
                targetConnection.close();
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        
    }//drain
         

    }
}