package br.com.drbsoft.rabbitmq.client.api;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQTemplate {

	private ConnectionFactory factory;

	public RabbitMQTemplate(ConnectionFactory factory) {
		this.factory = factory;
	}

	public void convertAndSend(String exchangeName, String queueName, String message) throws IOException, TimeoutException {
		if(message == null){
			throw new IllegalArgumentException("Message cannot be null!");
		}
		Connection connection = null;
		Channel channel = null;
		try {
		    connection = factory.newConnection();
		    channel = connection.createChannel();

		    channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC, true, false, null);
		    channel.queueDeclare(queueName, true, false, false, null);
		    
		    channel.queueBind(queueName, exchangeName, "");
		    channel.basicPublish(exchangeName, "", null, message.getBytes("UTF-8"));

		} catch (IOException e) {
			throw e;
		} catch (TimeoutException e) {
			throw e;
		} finally {
			if(channel != null){
				channel.close();
			}
			if(connection != null){
				connection.close();
			}
		}
		
	}
}
