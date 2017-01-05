package com.rebelai.wallace.memory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.rebelai.wallace.Journal;

public class InMemoryJournal implements Journal {
	private static final Logger LOG = LoggerFactory.getLogger(InMemoryJournal.class);
	
	private final BlockingQueue<byte[]> messages;
	private final AtomicBoolean isClosed = new AtomicBoolean(true);
	private final int maxQueuedMessages;
	
	public InMemoryJournal(int maxQueuedMessages){
		this.maxQueuedMessages = maxQueuedMessages;
		messages = new LinkedBlockingQueue<>(maxQueuedMessages);
	}

	@Override
	public void close() throws IOException {
		if(isClosed.compareAndSet(false, true)){
			while(!messages.isEmpty()){
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		if(isClosed.compareAndSet(false, true)){
			long eTime = System.currentTimeMillis()+unit.toMillis(timeout);
			while(!messages.isEmpty()){
				try {
					Thread.sleep(1);
					if(eTime <= System.currentTimeMillis()){
						break;
					}
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}

	@Override
	public void open() throws IOException {
		isClosed.set(false);
	}

	@Override
	public boolean isClosed() {
		return isClosed.get();
	}

	@Override
	public CompletableFuture<Void> write(byte[] bytes) {
		Preconditions.checkNotNull(bytes);
		if(bytes.length <= 0){
			CompletableFuture<Void> f= new CompletableFuture<>();
			f.completeExceptionally(new IOException("Message doesn't contain the requested amount of bytes"));
			return f;
		}
		
		this.messages.add(bytes);
		
		CompletableFuture<Void> f = new CompletableFuture<>();
		f.complete(null);
		return f;
	}

	@Override
	public CompletableFuture<Void> write(byte[] bytes, int offset, int length) {
		Preconditions.checkNotNull(bytes);
		if(bytes.length-offset < length){
			CompletableFuture<Void> f= new CompletableFuture<>();
			f.completeExceptionally(new IOException("Message doesn't contain the requested amount of bytes"));
			return f;
		}
		
		this.messages.add(Arrays.copyOfRange(bytes, offset, length));
		
		CompletableFuture<Void> f = new CompletableFuture<>();
		f.complete(null);
		return f;
	}

	@Override
	public CompletableFuture<Void> write(ByteBuffer buffer) {
		Preconditions.checkNotNull(buffer);
		if(buffer.position() != 0){
			buffer.flip();
		}
		if(buffer.remaining() <= 0){
			CompletableFuture<Void> f= new CompletableFuture<>();
			f.completeExceptionally(new IOException("Message doesn't contain any bytes to write"));
			return f;
		}
		
		
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		this.messages.add(bytes);
		
		CompletableFuture<Void> f = new CompletableFuture<>();
		f.complete(null);
		return f;
	}

	@Override
	public byte[] read() throws IOException, InterruptedException {
		return this.messages.poll();
	}

	@Override
	public byte[] read(long timeout, TimeUnit timeUnit) throws IOException, InterruptedException {
		return this.messages.poll(timeout, timeUnit);
	}

	@Override
	public List<byte[]> readN(int numOfMessages) throws IOException, InterruptedException {
		if(numOfMessages > maxQueuedMessages){
			LOG.warn("Requested more messages then the buffer can hold. This may impact read performance by increasing blocking chances.");
		}
		final ImmutableList.Builder<byte[]> msgs = ImmutableList.builder();
		
		int i=0;
		while(!this.isClosed() && i<numOfMessages){
			try {
				byte[] msg = this.messages.poll(10, TimeUnit.MILLISECONDS);
				if(msg != null){
					msgs.add(msg);
					i++;
				}
			} catch(InterruptedException e){
				break;
			}
		}
		
		
		//If the reader has been closed we should drain everything
		if(this.isClosed() && i<numOfMessages){
			List<byte[]> remainder = new ArrayList<>();
			this.messages.drainTo(remainder);
			msgs.addAll(remainder);
		}
		
		return msgs.build();
	}

	@Override
	public Map<String, HealthCheck> getHealthChecksWarning() {
		ImmutableMap.Builder<String, HealthCheck> healthBuilder = ImmutableMap.builder();
		healthBuilder.put("ReaderBufferCapacity", new HealthCheck(){
			@Override
			protected Result check() throws Exception {
				if(InMemoryJournal.this.getQueuedMessagePercentFull() > 75){
					return Result.unhealthy("Message buffer is > 75% full");
				}
				return Result.healthy();
			}
		});
		return healthBuilder.build();
	}

	@Override
	public Map<String, HealthCheck> getHealthChecksError() {
		ImmutableMap.Builder<String, HealthCheck> healthBuilder = ImmutableMap.builder();
		
		healthBuilder.put("JournalStopped", new HealthCheck(){
			@Override
			protected Result check() throws Exception {
				if(InMemoryJournal.this.isClosed()){
					return Result.unhealthy("Journal has been stopped or closed");
				}
				return Result.healthy();
			}
		});
		
		return healthBuilder.build();
	}

	@Override
	public Map<String, Metric> getMetrics() {
		return ImmutableMap.of("BufferedMessages", new Gauge<Integer>(){
			@Override
			public Integer getValue() {
				return InMemoryJournal.this.messages.size();
			}
		});
	}
	public int getQueuedMessagePercentFull(){
		float currentSize = messages.size();
		return (int) ((currentSize/maxQueuedMessages)*100);
	}

	@Override
	public long totalMessages() {
		return messages.size();
	}

	@Override
	public long readMessages() {
		return 0;
	}

	@Override
	public long unreadMessages() {
		return messages.size();
	}

	@Override
	public int queuedWrites() {
		return 0;
	}

	@Override
	public int queuedReads() {
		return messages.size();
	}
}
