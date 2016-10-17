package com.rebelai.wallace.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Writer implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
	final FileSystemJournal journal;
	
	private final long maxSegmentSize;
	private AtomicLong currentFileSize = new AtomicLong(0);
	private AsynchronousFileChannel channel;
	private BlockingQueue<ByteBuffer> messagesToWrite;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private AtomicBoolean isWriting = new AtomicBoolean();
	private boolean isClosed = true;
	
	private CompletionHandler<Integer, ByteBuffer> handler = new CompletionHandler<Integer, ByteBuffer>(){
		@Override
		public void completed(Integer result, ByteBuffer attachment) {
			long s = currentFileSize.addAndGet(result);
			if(attachment.hasRemaining()){
				channel.write(attachment, s, attachment, this);
			} else {
				if(s >= maxSegmentSize){
					try {
						Writer.this.closeChannel();
						Writer.this.open(journal.newJournal());
					} catch (IOException e) {
						LOG.error("Failed to close channel", e);
					}
				}
				
				if(!Writer.this.isClosed()){
					final ByteBuffer b = messagesToWrite.poll();
					
					if(b != null){
						channel.write(b, s, b, this);
					} else {
						isWriting.set(false);
					}
				}
			}
		}
		@Override
		public void failed(Throwable exc, ByteBuffer attachment) {
			LOG.error("Failed to write to channel", exc);
			try {
				Writer.this.close();
			} catch(IOException e){
				LOG.error("Failed to close Writer", e);
			}
		}
	};
	
	protected Writer(final FileSystemJournal journal, final int queueCapacity, final long maxSegmentSize) throws IOException{
		this.journal = journal;
		this.maxSegmentSize = maxSegmentSize;
		messagesToWrite = new LinkedBlockingQueue<>(queueCapacity);
		this.open(journal.getLatestJournal());
		this.isClosed = false;
	}
	
	protected void open(Path file) throws IOException{
		lock.writeLock().lock();
		try {
			if(file == null){
				file = journal.newJournal();
			}
			
			channel = AsynchronousFileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			currentFileSize.set(channel.size());
			
			if(currentFileSize.get() >= maxSegmentSize){
				channel.close();
				channel = AsynchronousFileChannel.open(journal.newJournal(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
				currentFileSize.set(channel.size());
			}
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	public boolean isClosed(){
		return isClosed || this.isChannelClosed();
	}
	
	protected boolean isChannelClosed(){
		lock.readLock().lock();
		boolean closed = (channel == null || !channel.isOpen());
		lock.readLock().unlock();
		return closed;
	}
	
	public void write(ByteBuffer buffer) throws IOException{
		if(isClosed()){
			throw new IOException("Journal has been closed");
		}
		
		if(isWriting.getAndSet(true)){
			messagesToWrite.add(buffer);
		} else {
			channel.write(buffer, currentFileSize.get(), buffer, handler);
		}
	}
	
	protected void closeChannel() throws IOException{
		if(!isChannelClosed()){
			lock.writeLock().lock();
			try {
				channel.close();
				channel = null;
			} finally {
				lock.writeLock().unlock();
			}
		}
	}

	@Override
	public void close() throws IOException {
		//TODO: Flush buffer
		if(!isClosed()){
			this.isClosed = true;
			if(!this.isWriting.get()){
				closeChannel();
			}
		}
	}
}
