package wdsr.exercise4;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class main {
	private static final Logger log = LoggerFactory.getLogger(main.class);
	private final static String connectionUri = "tcp://localhost:61616";
	private static Connection connectionToBroker;
	private static Session session;
	private static Topic destination;
	private static MessageConsumer messageConsumer;
	private static int quantityOfMessage = 0;
	
	public static void main(String[] args) {
		
		try {
			connectionToBroker = new ActiveMQConnectionFactory(connectionUri).createConnection();
			connectionToBroker.setClientID("2017");
			connectionToBroker.start();
			session = connectionToBroker.createSession(false, Session.AUTO_ACKNOWLEDGE);
			destination = session.createTopic("slabykonrad.TOPIC");
			messageConsumer = session.createDurableSubscriber(destination, "slaby");
			
			messageConsumer.setMessageListener(new MessageListener() {
				
				@Override
				public void onMessage(Message message) {
					try {
						log.info(((TextMessage) message).getText());
						++quantityOfMessage;
					} catch (JMSException e) {
						log.error(e.getMessage());
					}
				}
			});
			log.info("Number of messages: " + quantityOfMessage);
			
			session.close();
			connectionToBroker.close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
		
	}

}
