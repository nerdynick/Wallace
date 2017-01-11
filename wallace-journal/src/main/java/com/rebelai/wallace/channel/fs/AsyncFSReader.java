package com.rebelai.wallace.channel.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableMap;
import com.rebelai.wallace.channel.AsyncJournal;
import com.rebelai.wallace.channel.AsyncJournalSegment;
import com.rebelai.wallace.channel.AsyncReader;

public class AsyncFSReader extends AsyncReader<AsynchronousFileChannel> {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncFSReader.class);
	
	protected AtomicLong currentFileOffset = new AtomicLong(0);

	protected AsyncFSReader(AsyncJournal<AsynchronousFileChannel> journal, int maxQueuedMessages, final int maxMessageSize, final int readBufferSize) throws IOException {
		super(journal, maxQueuedMessages, maxMessageSize, readBufferSize);
	}
	
	protected AsynchronousFileChannel _open(final AsyncJournalSegment<AsynchronousFileChannel> segment) throws IOException{
		LOG.debug("Opening Reader to {}", segment);
		AsynchronousFileChannel channel = segment.openRead();
		currentFileOffset.set(segment.readOffset());
		
		return channel;
	}

	@Override
	protected void _closeSegment() throws IOException {
		this.segment.closeRead();
		currentFileOffset.set(0);
	}

	@Override
	protected boolean _shouldRoll() {
		try {
			if(this.channel.size() <= this.currentFileOffset.get()){
				return true;
			}
		} catch (IOException e) {
			LOG.error("Failed to get file size");
		}
		return false;
	}

	@Override
	protected void read(int bytes) {
		currentFileOffset.addAndGet(bytes);
	}

	@Override
	protected void _read(ByteBuffer buffer, CompletionHandler<Integer, Void> handler, int plusBytes) {
		this.channel.read(buffer, currentFileOffset.get()+plusBytes, null, handler);
	}
	
	@Override
	public Map<String, Metric> getMetrics() {
		ImmutableMap.Builder<String, Metric> metBuilder = ImmutableMap.builder();
		metBuilder.putAll(super.getMetrics());
		metBuilder.put("CurrentOffset", new CachedGauge<Long>(100, TimeUnit.MILLISECONDS){
			@Override
			protected Long loadValue() {
				return currentFileOffset.get();
			}
		});
		return metBuilder.build();
	}
}
