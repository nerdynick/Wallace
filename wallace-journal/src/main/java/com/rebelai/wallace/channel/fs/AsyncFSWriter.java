package com.rebelai.wallace.channel.fs;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.rebelai.wallace.channel.AsyncJournal;
import com.rebelai.wallace.channel.AsyncJournalSegment;
import com.rebelai.wallace.channel.AsyncWriter;

public class AsyncFSWriter extends AsyncWriter<AsynchronousFileChannel> {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncFSWriter.class);
	
	private final long maxSegmentSize;
	private AtomicLong currentFileSize = new AtomicLong(0);
	
	public AsyncFSWriter(final AsyncJournal<AsynchronousFileChannel> journal, final int queueCapacity, final long maxSegmentSize, final int maxMessageSize) throws IOException{
		super(journal, queueCapacity, maxMessageSize);
		
		Preconditions.checkArgument(maxSegmentSize > 0);
		Preconditions.checkArgument(maxSegmentSize <= Long.MAX_VALUE);
		this.maxSegmentSize = maxSegmentSize;
		LOG.debug("Max Segment size is: {}", maxSegmentSize);
	}
	
	protected AsynchronousFileChannel _open(final AsyncJournalSegment<AsynchronousFileChannel> segment) throws IOException{
		LOG.debug("Opening Writer to {}", segment);
		AsynchronousFileChannel channel = segment.openWrite();
		currentFileSize.set(channel.size());
		
		
		if(currentFileSize.get() >= maxSegmentSize){
			LOG.debug("File already appears to have reached it's limit. Opening a new one.");
			segment.closeWrite();
			this.segment = this.journal.getNew();
			return this._open(this.segment);
		}
		
		return channel;
	}
	
	protected void _closeSegment() throws IOException{
		this.segment.closeWrite();
		currentFileSize.set(0);
	}
	
	protected void write(final Message msg, CompletionHandler<Integer, Message> handler){
		channel.write(msg.buffer, currentFileSize.get(), msg, handler);
	}
	
	protected void wrote(final int bytes){
		currentFileSize.addAndGet(bytes);
	}
	
	protected boolean shouldRoll(){
		return (currentFileSize.get() >= maxSegmentSize);
	}
}
