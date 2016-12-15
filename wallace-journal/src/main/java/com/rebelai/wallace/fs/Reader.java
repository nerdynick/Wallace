package com.rebelai.wallace.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class Reader implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(Reader.class);
	
	private final FileSystemJournal journal;
	private final BlockingQueue<byte[]> queuedMessages;
	private int maxQueuedMessages;
	private boolean isClosed = false;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	private final long maxSegmentSize;
	private AsynchronousFileChannel channel;
	private AtomicLong currentFileSize = new AtomicLong(0);
	private Path currentChannel;
	
	
	
	
	protected Reader(final FileSystemJournal journal, final long maxSegmentSize) throws IOException{
		this(journal, 100, maxSegmentSize);
	}
	
	protected Reader(final FileSystemJournal journal, final int maxQueuedMessages, final long maxSegmentSize) throws IOException{
		this.journal = journal;
		this.queuedMessages = new LinkedBlockingQueue<>(maxQueuedMessages);
		this.maxQueuedMessages = maxQueuedMessages;
		this.maxSegmentSize = maxSegmentSize;
		
		this.open(journal.getEarliestJournal());
	}
	
	protected void roll() throws IOException{
		this.deleteCurrentChannel();
		this.open(this.journal.getEarliestJournal());
	}
	
	protected void open(Path file) throws IOException{
		lock.writeLock().lock();
		try {
			if(file == null){
				//TODO: Time delay wait to open. No file exists to read yet
			}
	
			LOG.debug("Opening Reader to {}", file);
			channel = AsynchronousFileChannel.open(file, StandardOpenOption.READ);
			currentFileSize.set(channel.size());
			currentChannel = file;
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	protected void deleteCurrentChannel() throws IOException{
		if(channel != null){
			lock.writeLock().lock();
			try {
				channel.close();
				channel = null;
				
				currentFileSize.set(0);
				
				Files.delete(currentChannel);
				currentChannel = null;
			} finally {
				lock.writeLock().unlock();
			}
		}
	}
	
	public byte[] read() throws IOException, InterruptedException{
		return this.queuedMessages.take();
	}
	public byte[] read(final long timeout, final TimeUnit timeUnit) throws IOException, InterruptedException{
		return this.queuedMessages.poll(timeout, timeUnit);
	}
	public List<byte[]> readN(final int numOfMessages) throws IOException, InterruptedException{
		if(numOfMessages > maxQueuedMessages){
			LOG.info("Requested more messages then the buffer can hold. This may impact read performance by increasing blocking chances.");
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
		
		
		//If the reader has been closed we should drain everything
		if(this.isClosed() && i<numOfMessages){
			List<byte[]> remainder = new ArrayList<>();
			this.queuedMessages.drainTo(remainder);
			msgs.addAll(remainder);
		}
		
		return msgs.build();
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

	@Override
	public void close() throws IOException {
		if(!isClosed()){
			this.isClosed = true;
			closeChannel();
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

}
