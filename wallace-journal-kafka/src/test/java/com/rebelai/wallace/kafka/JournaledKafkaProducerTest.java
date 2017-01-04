package com.rebelai.wallace.kafka;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Charsets;
import com.rebelai.wallace.Journal;

public class JournaledKafkaProducerTest {

	@Test
	public void testSerDeseri() {
		Journal journal = Mockito.mock(Journal.class);
		Mockito.when(journal.isClosed()).thenReturn(false);
		@SuppressWarnings("unchecked")
		KafkaProducer<String,String> kafaproducer = Mockito.mock(KafkaProducer.class);
		
		JournaledKafkaProducer producer = new JournaledKafkaProducer(journal, kafaproducer);
		
		final String topic = "testTopic";
		final String key = "testKey";
		final String message = "testMessage";
		
		final ByteBuffer buffer = producer.write(topic.getBytes(Charsets.UTF_8), key.getBytes(Charsets.UTF_8), message.getBytes(Charsets.UTF_8));
		buffer.flip();
		final ProducerRecord<String, String> record = producer.read(buffer.array());
		producer.bufferPool.release(buffer);
		
		assertEquals(topic, record.topic());
		assertEquals(key, record.key());
		assertEquals(message, record.value());
		
		producer.close();
	}

}
