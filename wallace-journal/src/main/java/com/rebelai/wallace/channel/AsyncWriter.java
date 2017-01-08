package com.rebelai.wallace.channel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.eclipse.jetty.io.ByteBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.rebelai.wallace.BooleanLatch;
import com.rebelai.wallace.OversizedArrayByteBufferPool;

public abstract class AsyncWriter<T extends AsynchronousChannel> implements Closeable, MetricSet {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncWriter.class);
	private static final int headerByteSize = 4; //We only store the length as int in the headers
	
	final protected AsyncJournal<T> journal;
	final protected ByteBufferPool bufferPool;
	protected AsyncJournalSegment<T> segment;
	protected T channel;
	
	private BlockingQueue<Message> messagesToWrite;
	private final int queueCapacity;
	private final int maxMessageSize;
	private ReentrantReadWriteLock channeLock = new ReentrantReadWriteLock();
	private BooleanLatch isWriting = new BooleanLatch();
	private boolean isClosed = true;
	
	private final Map<String, Metric> metrics;
	private final Meter writerMeter = new Meter();
	private final Meter physicalWriterMeter = new Meter();
	
	protected AsyncWriter(final AsyncJournal<T> journal, final int queueCapacity, final int maxMessageSize) throws IOException{
		this.journal = journal;
		this.queueCapacity = queueCapacity;
		this.maxMessageSize = maxMessageSize;
		
		messagesToWrite = new LinkedBlockingQueue<>(queueCapacity);
		bufferPool = new OversizedArrayByteBufferPool(0, 1024, maxMessageSize);
		
		ImmutableMap.Builder<String, Metric> metBuilder = ImmutableMap.builder();
		metBuilder.put("MessageWriteRate", writerMeter);
		metBuilder.put("MessagePhysicalWriteRate", physicalWriterMeter);
		metBuilder.put("QueuedWriteMessages", new CachedGauge<Integer>(100, TimeUnit.MILLISECONDS){
			@Override
			protected Integer loadValue() {
				return AsyncWriter.this.getQueuedMessages();
			}
		});
		metrics = metBuilder.build();
	}
	
	@Override
	public Map<String, Metric> getMetrics() {
		return metrics;
	}
	
	public int getQueuedMessages(){
		return messagesToWrite.size();
	}
	public int getQueuedMessagePercentFull(){
		float currentSize = messagesToWrite.size();
		return (int) ((currentSize/queueCapacity)*100);
	}
	
	private CompletionHandler<Integer, Message> handler = new CompletionHandler<Integer, Message>(){
		@Override
		public void completed(Integer result, Message attachment) {
			AsyncWriter.this.wrote(result.intValue());
			if(attachment.buffer.hasRemaining()){
				LOG.debug("Writing isn't complete wrote {} bytes", result);
				AsyncWriter.this.write(attachment, this);
			} else {
				LOG.debug("Writing is complete wrote {} bytes", result);
				physicalWriterMeter.mark();
				AsyncWriter.this.segment.msgCountIncr();
				
				if(AsyncWriter.this.shouldRoll()){
					try {
						LOG.debug("Rolling segment");
						AsyncWriter.this.roll();
					} catch (IOException e) {
						LOG.error("Failed to close channel", e);
					}
				}
				
				LOG.debug("Completing Future");
				attachment.complete(null); //Trigger any listening events. This should be async and non blocking

				synchronized(AsyncWriter.this){
					final Message b = messagesToWrite.poll();
					if(b != null){
						LOG.debug("Writing next message in queue");
						AsyncWriter.this.write(b, this);
					} else {
						LOG.debug("No messages to write.");
						isWriting.release();
					}
				}
			}
		}
		@Override
		public void failed(Throwable exc, Message attachment) {
			LOG.error("Failed to write to channel. Closing writer", exc);
			try {
				AsyncWriter.this.close();
			} catch(IOException e){
				LOG.error("Failed to close Writer", e);
			}
		}
	};
	
	
	
	private void write(final Message msg){
		if(msg != null){
			writerMeter.mark();
			synchronized(this){
				if(isWriting.tryAquire()){
					LOG.debug("Nothing queued to write. Starting write.");
					this.write(msg, handler);
				} else {
					LOG.debug("Adding write to Queue of writes");
					messagesToWrite.add(msg);
				}
			}
		}
	}
	
	protected abstract void write(final Message msg, CompletionHandler<Integer, Message> handler);
	protected abstract void wrote(final int bytes);
	protected abstract boolean shouldRoll();

	
	public synchronized void start() throws IOException{
		this.isClosed = false;
		this.open();
	}
	
	protected void roll() throws IOException{
		LOG.debug("Rolling Journal file from {}", this.segment);
		channeLock.writeLock().lock();
		try {
			this._closeSegment();
			this.segment = null;
			this.channel = null;
			this.segment = this.journal.getNew();
			this.channel = this._open(this.segment);
		} finally{
			channeLock.writeLock().unlock();
		}
		LOG.debug("New journal file is {}", segment);
	}
	
	protected void open() throws IOException{
		channeLock.writeLock().lock();
		try {
			LOG.debug("Opening last journal");
			this.segment = this.journal.getLast();
			this.channel = _open(this.segment);
		} finally {
			channeLock.writeLock().unlock();
		}
	}
	
	
	protected abstract T _open(final AsyncJournalSegment<T> segment) throws IOException;
	protected abstract void _closeSegment() throws IOException;
	
	protected void closeSegment() throws IOException{
		if(!isChannelClosed()){
			channeLock.writeLock().lock();
			try {
				_closeSegment();
				this.segment = null;
				this.channel = null;
			} finally {
				channeLock.writeLock().unlock();
			}
		}
	}
	
	public boolean isClosed(){
		return isClosed || this.isChannelClosed();
	}
	
	protected boolean isChannelClosed(){
		channeLock.readLock().lock();
		boolean closed = (channel == null || !channel.isOpen());
		channeLock.readLock().unlock();
		return closed;
	}
	
	private void _validate(final int msgSize) throws IOException{
		if(msgSize <= 0){
			throw new IOException("Message doesn't contain any bytes to write");
		}
		
		if(msgSize > maxMessageSize){
			LOG.warn("Message is larger then max: Size={} Max={}", msgSize, maxMessageSize);
		}
	}
	
	public CompletableFuture<Void> write(final byte[] bytes, int offset, int length){
		Preconditions.checkNotNull(bytes);
		if(bytes.length-offset < length){
			CompletableFuture<Void> f= new CompletableFuture<>();
			f.completeExceptionally(new IOException("Message doesn't contain the requested amount of bytes"));
			return f;
		}
		
		try {
			_validate(length);
		} catch (Exception e){
			CompletableFuture<Void> f= new CompletableFuture<>();
			f.completeExceptionally(e);
			return f;
		}
		
		final ByteBuffer b = bufferPool.acquire(headerByteSize+length, false);
		b.clear();
		b.putInt(length);
		b.put(bytes, offset, length);
		b.flip();
		return _write(b)
				.whenComplete((v,e)->this.bufferPool.release(b));
	}
	public CompletableFuture<Void> write(final ByteBuffer buffer){
		Preconditions.checkNotNull(buffer);
		if(buffer.position() != 0){
			buffer.flip();
		}
		
		try {
			_validate(buffer.remaining());
		} catch (Exception e){
			CompletableFuture<Void> f= new CompletableFuture<>();
			f.completeExceptionally(e);
			return f;
		}
		
		final ByteBuffer b = bufferPool.acquire(headerByteSize+buffer.remaining(), false);
		b.clear();
		b.putInt(buffer.remaining());
		b.put(buffer);
		b.flip();
		buffer.flip();
		LOG.debug("Prepared to write {} bytes with original of {} bytes", b.remaining(), buffer.remaining());
		return _write(b)
				.whenComplete((v,e)->this.bufferPool.release(b));
	}
	
	public CompletableFuture<Void> _write(ByteBuffer buffer){
		LOG.debug("Received write message");
		if(isClosed()){
			final CompletableFuture<Void> future = new CompletableFuture<>();
			future.completeExceptionally(new IOException("Journal has been closed or not opened yet"));
			return future;
		}

		Message msg = new Message(buffer);
		write(msg);
		return msg;
	}

	@Override
	public void close() throws IOException {
		if(!isClosed()){
			this.isClosed = true;
			LOG.debug("Draining write queue and waiting");
			_drainAndWait();
			
			try {
				LOG.debug("Writing is closed. Closing");
				this.closeSegment();
			} catch (IOException e) {
				LOG.error("Failed to close segment cleanly");
			}
		}
	}
	
	public void _drainAndWait(){
		LOG.debug("Attempting a kicker to the work as a just incase");
		this.write(this.messagesToWrite.poll());
		
		try {
			LOG.debug("Waiting for write queue to be drained");
			this.isWriting.await();
			if(!this.messagesToWrite.isEmpty()){
				_drainAndWait();
			}
		} catch (InterruptedException e) {
			LOG.error("Inturupt while waiting for write queue to drain to disk.");
		}
	}
	
	public void await() throws InterruptedException{
		this.isWriting.await();
		LOG.debug("Queue has {} elements", this.messagesToWrite.size());
	}
	public void await(long timeout, TimeUnit unit) throws InterruptedException{
		this.isWriting.await(timeout, unit);
		LOG.debug("Queue has {} elements", this.messagesToWrite.size());
	}
	
	public static class Message extends CompletableFuture<Void>{
		final public ByteBuffer buffer;
		public Message(final ByteBuffer buffer){
			this.buffer = buffer;
		}
	}
}