package com.rebelai.wallace.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.eclipse.jetty.io.ByteBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.rebelai.wallace.Journal;

public class JournaledKafkaProducer implements Producer<String,String>, MetricSet {
	private static final Logger LOG = LoggerFactory.getLogger(JournaledKafkaProducer.class);
	
	private static AtomicInteger threadInt  = new AtomicInteger(1);
	private static final int headerByteSize = 4*3; //We store the length of all 3 parts
	
	protected KafkaProducer<String,String> producer;
	protected Journal journal;
	protected ByteBufferPool bufferPool;
	private boolean isClosed = false;
	private Thread readThread;
	
	public JournaledKafkaProducer(Journal journal, KafkaProducer<String,String> producer) {
		this.producer = producer;
		this.journal = journal;
		
		bufferPool = new ArrayByteBufferPool(0, 1024, 1024*5);
		readThread = new Thread(new ReadRunnable(), "journaled-kafka-reader-"+threadInt.getAndIncrement());	
    }
	
	public void start(){
		readThread.start();
	}
	
	public CompletableFuture<RecordMetadata> send(String topic, String key, byte[] message) {
		final byte[] topicBytes = Preconditions.checkNotNull(topic).getBytes(Charsets.UTF_8);
		final byte[] keyBytes = Preconditions.checkNotNull(key).getBytes(Charsets.UTF_8);
		final ByteBuffer buffer = write(topicBytes, keyBytes, Preconditions.checkNotNull(message));
		
		return journal
					.write(buffer)
					.handleAsync((v,e)->{
						bufferPool.release(buffer);
						return null;
					});
	}
	
	public CompletableFuture<RecordMetadata> send(String topic, String key, String message) {
		return send(topic, key, Preconditions.checkNotNull(message).getBytes(Charsets.UTF_8));
	}
	
	public CompletableFuture<RecordMetadata> send(String topic, String key, String message, Callback callback) {
		return this
				.send(topic, key, message)
				.whenCompleteAsync((r,e)->callback.onCompletion(r, (Exception) e));
	}

	@Override
	public CompletableFuture<RecordMetadata> send(ProducerRecord<String, String> record) {
		return this.send(record.topic(), record.key(), record.value());
	}

	@Override
	public CompletableFuture<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
		Preconditions.checkNotNull(record);
		Preconditions.checkNotNull(callback);
		return this.send(record.topic(), record.key(), record.value(), callback);
	}

	@Override
	public void flush() {
		producer.flush();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return producer.partitionsFor(topic);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return this.producer.metrics();
	}

	@Override
	public void close() {
		if(!isClosed()){
			isClosed=true;
			try {
				journal.close();
			} catch (IOException e) {
				LOG.error("Failed to close journal properly", e);
			}
			producer.close();
		}
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		if(!isClosed()){
			isClosed=true;
			try {
				journal.close();
			} catch (IOException e) {
				LOG.error("Failed to close journal properly", e);
			}
			producer.close(timeout, unit);
		}
	}
	
	public boolean isClosed(){
		return isClosed || journal.isClosed();
	}
	
	protected ByteBuffer write(byte[] topic, byte[] key, byte[] message){
		int size = headerByteSize+topic.length+key.length+message.length;
		final ByteBuffer buffer = bufferPool.acquire(size, false);
		buffer.clear();
		buffer.putInt(topic.length);
		buffer.putInt(key.length);
		buffer.putInt(message.length);
		buffer.put(topic);
		buffer.put(key);
		buffer.put(message);
		buffer.flip();
		
		return buffer;
	}
	
	protected ProducerRecord<String, String> read(byte[] message){
		final ByteBuffer bufferedMessage = JournaledKafkaProducer.this.bufferPool.acquire(message.length, false);
		bufferedMessage.clear();
		bufferedMessage.put(message);
		bufferedMessage.flip();
		
		int topicSize = bufferedMessage.getInt();
		int keySize = bufferedMessage.getInt();
		int messageSize = bufferedMessage.getInt();
		
		byte[] topicByte = new byte[topicSize];
		byte[] keyByte = new byte[keySize];
		byte[] messageByte =new byte[messageSize];
		
		bufferedMessage
			.get(topicByte)
			.get(keyByte)
			.get(messageByte);
		
		JournaledKafkaProducer.this.bufferPool.release(bufferedMessage);
		
		return new ProducerRecord<>(new String(topicByte,Charsets.UTF_8), new String(keyByte,Charsets.UTF_8), new String(messageByte,Charsets.UTF_8));
	}
	
	private class ReadRunnable implements Runnable{
		@Override
		public void run() {
			while(!JournaledKafkaProducer.this.journal.isClosed()){
				byte[] msg = null;
				try {
					msg = JournaledKafkaProducer.this.journal.read(5, TimeUnit.MILLISECONDS);
				} catch (IOException | InterruptedException e1) {
					LOG.warn("Failed to read messages. Will attempt more reads.", e1);
				}
				
				if(msg != null){
					final ProducerRecord<String, String> record = JournaledKafkaProducer.this.read(msg);
					
					JournaledKafkaProducer.this.producer.send(record);
				} else {
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						LOG.warn("Interrupt while waiting for messages. Will attempt more reads.");
					}
				}
			}
			LOG.info("Journal has been closed. Shutting down kafka read thread.");
		}
		
	}

	@Override
	public Map<String, com.codahale.metrics.Metric> getMetrics() {
		ImmutableMap.Builder<String, com.codahale.metrics.Metric> metrics = ImmutableMap.builder();
		metrics.putAll(this.journal.getMetrics());
		
		for(Map.Entry<MetricName, ? extends Metric> met: this.metrics().entrySet()){
			final Metric m = met.getValue();
			metrics.put(MetricRegistry.name("producer",  met.getKey().group(), met.getKey().name()), new Gauge<Double>(){
				@Override
				public Double getValue() {
					return Double.valueOf(m.value());
				}
			});
		}
		
		return metrics.build();
	}
	public Map<String, HealthCheck> getHealthChecksWarning(){
		ImmutableMap.Builder<String, HealthCheck> checks = ImmutableMap.builder();
		checks.putAll(this.journal.getHealthChecksWarning());
		
		return checks.build();
	}
	public Map<String, HealthCheck> getHealthChecksError(){
		ImmutableMap.Builder<String, HealthCheck> checks = ImmutableMap.builder();
		checks.putAll(this.journal.getHealthChecksWarning());
		
		return checks.build();
	}
}
