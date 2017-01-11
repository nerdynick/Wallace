package com.rebelai.wallace.channel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.rebelai.wallace.BooleanLatch;
import com.rebelai.wallace.OversizedArrayByteBufferPool;

public abstract class AsyncReader<T extends AsynchronousChannel> implements Closeable, MetricSet {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncReader.class);
	private static final int headerByteSize = 4; //We only store the length as int in the headers
	
	private final AsyncJournal<T> journal;
	protected AsyncJournalSegment<T> segment;
	protected T channel;
	final protected ByteBufferPool bufferPool;
	final private ByteBuffer readBuffer;
	final private ByteBuffer lengthBuffer = ByteBuffer.allocate(headerByteSize);
	private ByteBuffer _currentMessage;
	private int _currentMessageLength = 0;
	
	private final BlockingQueue<byte[]> queuedMessages;
	private final int maxQueuedMessages;
	private boolean isClosed = true;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private BooleanLatch isReading = new BooleanLatch();
	
	private final Map<String, Metric> metrics;
	private final Meter msgReadMeter = new Meter();
	private final Meter physicalReadMeter = new Meter();
	
	protected AsyncReader(final AsyncJournal<T> journal, final int maxQueuedMessages, final int maxMessageSize, final int readBufferSize) throws IOException{
		this.journal = journal;
		this.maxQueuedMessages = maxQueuedMessages;
		this.queuedMessages = new LinkedBlockingQueue<>(maxQueuedMessages);
		bufferPool = new OversizedArrayByteBufferPool(0, 1024, maxMessageSize);
		readBuffer = ByteBuffer.allocate(readBufferSize);
		
		ImmutableMap.Builder<String, Metric> metBuilder = ImmutableMap.builder();
		metBuilder.put("MessageReadRate", msgReadMeter);
		metBuilder.put("PhysicalReadRate", physicalReadMeter);
		metBuilder.put("QueuedReadMessages", new CachedGauge<Integer>(100, TimeUnit.MILLISECONDS){
			@Override
			protected Integer loadValue() {
				return AsyncReader.this.getQueuedMessages();
			}
		});
		metrics = metBuilder.build();
	}
	public int getQueuedMessagePercentFull(){
		float currentSize = queuedMessages.size();
		return (int) ((currentSize/maxQueuedMessages)*100);
	}
	
	@Override
	public Map<String, Metric> getMetrics() {
		return metrics;
	}
	
	public AsyncJournalSegment<T> currentSegment(){
		return segment;
	}
	
	private CompletionHandler<Integer, Void> handler = new CompletionHandler<Integer, Void>(){
		private void resetCurrentMessage(){
			lengthBuffer.clear();
			_currentMessageLength = 0;
			if(_currentMessage != null){
				AsyncReader.this.bufferPool.release(_currentMessage);
				_currentMessage = null;
			}
		}
		
		@Override
		public void completed(Integer result, Void attachment) {
			physicalReadMeter.mark();
			if(isClosed){
				LOG.info("Closed stopping read");
				readBuffer.clear();
				resetCurrentMessage();
				return;
			}
			
			if(result > 0){
				LOG.debug("Read {} bytes.", result);
				readBuffer.flip();
				while(readBuffer.remaining() > 0){
					byte val = readBuffer.get();
					if(_currentMessageLength <= 0){
						lengthBuffer.put(val);
						if(lengthBuffer.remaining() == 0){
							lengthBuffer.flip();
							_currentMessageLength = lengthBuffer.getInt(0);
							LOG.debug("Length: {}", Integer.valueOf(_currentMessageLength));
							if(_currentMessageLength <= 0 || _currentMessageLength > 1024*5){
								LOG.warn("Possible Message length issue: {}", Integer.valueOf(_currentMessageLength));
							}
							_currentMessage = bufferPool.acquire(_currentMessageLength, false);
							_currentMessage.clear();
						}
					} else {
						_currentMessage.put(val);
						if(_currentMessage.position() == _currentMessageLength){
							LOG.debug("Full Message Read");
							byte[] msg = new byte[_currentMessageLength];
							_currentMessage.position(0);
							_currentMessage.get(msg);
							resetCurrentMessage();
							
							try {
								LOG.debug("Putting on Queue");
								queuedMessages.put(msg);
//								LOG.debug("Updating Offset and Count {}, {}", AsyncReader.this.segment.readOffset(), AsyncReader.this.segment.msgReadCount());
								AsyncReader.this.segment.msgReadCountIncr();
								AsyncReader.this.segment.readOffsetIncr(4+msg.length);
								AsyncReader.this.read(4+msg.length);
								msgReadMeter.mark();
							} catch (InterruptedException e) {
								LOG.warn("Failed to add message to queue. Was interupted while waiting.");
							}
						}
					}
				}
				LOG.debug("Finished parsing buffer");
				readBuffer.clear();
				if(!isClosed){
					AsyncReader.this._read();
				} else {
					LOG.info("Reader has been closed. Shutting read down.");
					isReading.release();
				}
			} else {
				//We didn't read anything. Most likely at the end of file or reading from a currently active file
				readBuffer.clear();
				if(!isClosed){
					if(AsyncReader.this.shouldRoll()){
						LOG.debug("Rolling segment");
						try {
							lengthBuffer.clear();
							resetCurrentMessage();
							
							AsyncReader.this.roll();
							LOG.debug("Rolling done. Starting read from new file");
							AsyncReader.this._read();
						} catch(IOException e){
							LOG.error("Failed to roll segment", e);
							isReading.release();
						}
					} else {
						LOG.trace("Waiting on segment writes");
						try {
							Thread.sleep(5);
							//We release the lock since we are just tailing the file. 
							//This lets anyone waiting to be woken up after these long periods of sleep.
							//But we must do it after the sleep otherwise we can wind up with a number of 
							//IO Threads being created due to multiple starts and sleeps
							isReading.release();
							AsyncReader.this.startRead();
						} catch (InterruptedException e) {
							LOG.warn("Interrupted while waiting for writes");
						}
					}
				} else {
					LOG.info("Reader has been closed. Shutting read down.");
					lengthBuffer.clear();
					resetCurrentMessage();
					isReading.release();
				}
			}
		}
		@Override
		public void failed(Throwable exc, Void attachment) {
			LOG.error("Failed to read segment", exc);
			isReading.release();
		}
	};
	
	protected boolean shouldRoll(){
		if(!this.segment.isOpenForWriting() && this._shouldRoll()){
			return true;
		}
		return false;
	}
	
	private void _read(){
		int plusBytes = 0;
		if(_currentMessageLength > 0){
			plusBytes+= 4;
		} else {
			plusBytes+=lengthBuffer.position();
		}
		if(_currentMessage != null){
//			LOG.debug("POS: {}", _currentMessage.position());
			plusBytes += _currentMessage.position();
		}
		
		LOG.debug("Reading next message from {} + {} bytes", AsyncReader.this.segment.readOffset(), plusBytes);
		_read(readBuffer, handler, plusBytes);
	}
	
	protected abstract boolean _shouldRoll();
	protected abstract void read(final int bytes);
	protected abstract void _read(ByteBuffer buffer, CompletionHandler<Integer, Void> handler, int plusBytes);
	
	public int getQueuedMessages(){
		return queuedMessages.size();
	}
	
	public synchronized void start() throws IOException{
		this.isClosed = false;
		this.readBuffer.clear();
		this.lengthBuffer.clear();
		this.open();
		this.startRead();
	}
	
	protected synchronized void startRead(){
		if(!isClosed){
			if(isReading.tryAquire()){
				LOG.trace("Starting read: {}", isReading.getState());
				this._read();
			} else {
				LOG.debug("Already reading");
			}
		} else {
			LOG.debug("Reader has been closed");
		}
	}
	
	protected void roll() throws IOException{
		LOG.debug("Rolling Journal file from {}", this.segment);
		lock.writeLock().lock();
		try {
			this._closeSegment();
			if(this.segment.canBeRemoved()){
				LOG.debug("Removing segment from journal");
				this.journal.removeSegment(this.segment);
			}
			
			this.segment = null;
			this.channel = null;
			this.segment = this.journal.getFirst();
			this.channel = this._open(this.segment);
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	protected void open() throws IOException{
		lock.writeLock().lock();
		try {
			this.segment = this.journal.getFirst();
			this.channel = _open(this.segment);
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	protected abstract T _open(final AsyncJournalSegment<T> segment) throws IOException;
	protected abstract void _closeSegment() throws IOException;
	
	public byte[] read() throws NoSuchElementException{
		startRead();
		return this.queuedMessages.remove();
	}
	public byte[] read(final long timeout, final TimeUnit timeUnit) throws IOException, InterruptedException{
		startRead();
		return this.queuedMessages.poll(timeout, timeUnit);
	}
	public List<byte[]> readN(final int numOfMessages) throws IOException, InterruptedException{
		startRead();
		if(numOfMessages > maxQueuedMessages){
			LOG.warn("Requested more messages then the buffer can hold. This may impact read performance by increasing blocking chances.");
		}
		final ImmutableList.Builder<byte[]> msgs = ImmutableList.builder();
		
		int i=0;
		while(!this.isClosed() && i<numOfMessages){
			try {
				byte[] msg = this.queuedMessages.poll(10, TimeUnit.MILLISECONDS);
				if(msg != null){
					msgs.add(msg);
					i++;
				}
			} catch(InterruptedException e){
				break;
			}
		}
		
		return msgs.build();
	}

	public Collection<byte[]> drain() {
		List<byte[]> remainder = new ArrayList<>();
		this.queuedMessages.drainTo(remainder);
		return remainder;
	}
	
	public boolean isClosed(){
		return isClosed || isChannelClosed(); 
	}
	
	protected boolean isChannelClosed(){
		lock.readLock().lock();
		boolean closed = (channel == null || !channel.isOpen());
		lock.readLock().unlock();
		return closed;
	}

	@Override
	public void close() throws IOException {
		if(!isClosed()){
			this.isClosed = true;
			closeSegment();
		}
	}
	
	public void close(long timeout, TimeUnit unit) {
		if(!isClosed()){
			this.isClosed = true;
			try {
				closeSegment();
			} catch (IOException e) {
				LOG.debug("Failed to close segment", e);
			}
		}
	}
	
	protected void closeSegment() throws IOException{
		if(!isChannelClosed()){
			lock.writeLock().lock();
			try {
				_closeSegment();
				this.segment = null;
				this.channel = null;
			} finally {
				lock.writeLock().unlock();
			}
		}
	}
	
	public void await() throws InterruptedException{
		LOG.debug("Looking to wait. State: {}", this.isReading.getState());
		this.isReading.await();
		LOG.debug("Queue has {} elements", this.queuedMessages.size());
	}
	public void await(long timeout, TimeUnit unit) throws InterruptedException{
		LOG.debug("Looking to wait. State: {}", this.isReading.getState());
		this.isReading.await(timeout, unit);
		LOG.debug("Queue has {} elements", this.queuedMessages.size());
	}
}
