package com.rebelai.wallace;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricSet;
import com.codahale.metrics.health.HealthCheck;

public interface Journal extends Closeable, MetricSet {
	public void open() throws IOException;
	public boolean isClosed();
	public CompletableFuture<Void> write(final byte[] bytes);
	public CompletableFuture<Void> write(final byte[] bytes, int offset, int length);
	public CompletableFuture<Void> write(final ByteBuffer buffer);
	
	public byte[] read() throws IOException, InterruptedException;
	public byte[] read(final long timeout, final TimeUnit timeUnit) throws IOException, InterruptedException;
	/**
	 * Fetch N number of messages from the buffer. This will block and wait for new messages to be buffered.
	 * If the journal is closed and the buffer contains messages. Those messages will be drained into the return. 
	 * This can result in more messages being returned then originally asked for. The intent of this is to allow post close
	 * the ability to drain the buffer, reducing the chance of lost messages.
	 * 
	 * @param numOfMessages
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public List<byte[]> readN(final int numOfMessages) throws IOException, InterruptedException;
	
	public void close(long timeout, TimeUnit unit);
	public Map<String, HealthCheck> getHealthChecksWarning();
	public Map<String, HealthCheck> getHealthChecksError();
	
	public long totalMessages();
	public long readMessages();
	public long unreadMessages();
	public int queuedWrites();
	public int queuedReads();
}
