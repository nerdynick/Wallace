package com.rebelai.wallace;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricSet;
import com.codahale.metrics.health.HealthCheck;

public interface Journal extends Closeable, MetricSet {
	public void open() throws IOException;
	public void close(long timeout, TimeUnit unit);
	public boolean isClosed();
	public boolean isWritingClosed();
	public boolean isReadingClosed();
	public CompletableFuture<Void> write(final byte[] bytes);
	public CompletableFuture<Void> write(final byte[] bytes, int offset, int length);
	public CompletableFuture<Void> write(final ByteBuffer buffer);
	
	public byte[] read() throws IOException, InterruptedException;
	public byte[] read(final long timeout, final TimeUnit timeUnit) throws IOException, InterruptedException;
	public List<byte[]> readN(final int numOfMessages) throws IOException, InterruptedException;
	public Collection<byte[]> drain();
	
	public Map<String, HealthCheck> getHealthChecksWarning();
	public Map<String, HealthCheck> getHealthChecksError();
	
	public long totalMessages();
	public long readMessages();
	public long unreadMessages();
	public int queuedWrites();
	public int queuedReads();
}
