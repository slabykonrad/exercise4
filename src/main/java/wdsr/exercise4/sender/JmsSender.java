package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;
	private final String connectionUri = "tcp://localhost:61616";
	
	private Connection connectionToBroker;
	private Session session;
	private Destination destination;
	private MessageProducer messageProducer;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
	}

	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		initialize(this.queueName);
		
		try {
			this.destination = session.createQueue(queueName);
			this.messageProducer = this.session.createProducer(this.destination);
			
			Order order = new Order(orderId,product,price);
			ObjectMessage message = this.session.createObjectMessage();
			message.setObject(order);
			message.setObject(order);
			message.setJMSType("Order");
			message.setStringProperty("WDSR-System","OrderProcessor");
			this.messageProducer.send(message);
			this.session.close();
			this.connectionToBroker.close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 * @throws JMSException 
	 */
	public void sendTextToQueue(String text) {
		initialize(this.queueName);
		
		try {
			this.destination = session.createQueue(queueName);
			this.messageProducer = this.session.createProducer(this.destination);
			
			TextMessage message = this.session.createTextMessage();
			message.setText(text);
			this.messageProducer.send(message);
			this.session.close();
			this.connectionToBroker.close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		initialize(this.topicName);
	        
		try {	
			this.destination = this.session.createTopic(this.topicName);
			this.messageProducer = this.session.createProducer(this.destination);
			MapMessage message = this.session.createMapMessage();
			
			for (Map.Entry<String, String> entry : map.entrySet()){
				message.setString(entry.getKey(), entry.getValue());
			}
		        
			this.messageProducer.send(message);
			this.session.close();
			this.connectionToBroker.close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}
	
	private void initialize(String queueName){
		try {
			ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(this.connectionUri);
			
			if(this.connectionToBroker == null){
				this.connectionToBroker = factory.createConnection();
			}
			
			if(this.session == null){
				this.session = connectionToBroker.createSession(false, Session.AUTO_ACKNOWLEDGE);
			}
			
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}
}
