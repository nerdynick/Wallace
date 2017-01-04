package com.rebelai.wallace.channel.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rebelai.wallace.channel.AsyncJournal;
import com.rebelai.wallace.channel.AsyncJournalSegment;
import com.rebelai.wallace.channel.AsyncReader;

public class AsyncFSReader extends AsyncReader<AsynchronousFileChannel> {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncFSReader.class);
	
	private AtomicLong currentFileSize = new AtomicLong(0);
	
	protected AsyncFSReader(AsyncJournal<AsynchronousFileChannel> journal) throws IOException {
		this(journal, 100);
	}

	protected AsyncFSReader(AsyncJournal<AsynchronousFileChannel> journal, int maxQueuedMessages) throws IOException {
		super(journal, maxQueuedMessages);
	}
	
	protected AsynchronousFileChannel _open(final AsyncJournalSegment<AsynchronousFileChannel> segment) throws IOException{
		LOG.debug("Opening Reader to {}", segment);
		AsynchronousFileChannel channel = segment.openRead();
		currentFileSize.set(segment.readOffset());
		
		return channel;
	}

	@Override
	protected void _closeSegment() throws IOException {
		this.segment.closeRead();
		currentFileSize.set(0);
	}

	@Override
	protected boolean _shouldRoll() {
		try {
			if(this.channel.size() <= this.currentFileSize.get()){
				return true;
			}
		} catch (IOException e) {
			LOG.error("Failed to get file size");
		}
		return false;
	}

	@Override
	protected void read(int bytes) {
		currentFileSize.addAndGet(bytes);
	}

	@Override
	protected void _read(ByteBuffer buffer, Message message, CompletionHandler<Integer, Message> handler) {
		this.channel.read(buffer, currentFileSize.get(), message, handler);
	}
}
