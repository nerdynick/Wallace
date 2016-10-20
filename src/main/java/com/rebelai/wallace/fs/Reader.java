package com.rebelai.wallace.fs;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class Reader implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(Reader.class);
	
	private final FileSystemJournal journal;
	private final BlockingQueue<byte[]> queuedMessages;
	private int maxQueuedMessages;
	private boolean isClosed = false;
	
	
	protected Reader(final FileSystemJournal journal){
		this(journal, 100);
	}
	
	protected Reader(final FileSystemJournal journal, final int maxQueuedMessages){
		this.journal = journal;
		this.queuedMessages = new LinkedBlockingQueue<>(maxQueuedMessages);
		this.maxQueuedMessages = maxQueuedMessages;
	}
	
	public byte[] read() throws IOException, InterruptedException{
		return queuedMessages.take();
	}
	public List<byte[]> readN(final int numOfMessages) throws IOException, InterruptedException{
		if(numOfMessages > maxQueuedMessages){
			LOG.info("Requested more messages then the buffer can hold. This may impact read performance by increasing blocking chances.");
		}
		final ImmutableList.Builder<byte[]> msgs = ImmutableList.builder();
		
		for(int i=0; i<numOfMessages; i++){
			while(!isClosed){
				byte[] msg = this.queuedMessages.poll(10, TimeUnit.MILLISECONDS);
				if(msg != null){
					msgs.add(this.queuedMessages.take());
				}
			}
		}
		
		return msgs.build();
	}

	@Override
	public void close() throws IOException {
		isClosed = true;
	}

}
