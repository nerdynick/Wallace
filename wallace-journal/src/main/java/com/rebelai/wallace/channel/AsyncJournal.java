package com.rebelai.wallace.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.collect.ImmutableMap;
import com.rebelai.wallace.Journal;
import com.rebelai.wallace.channel.fs.AsyncFSSegment;
import com.rebelai.wallace.health.PercentHealthCheck;

public abstract class AsyncJournal<T extends AsynchronousChannel> implements Journal {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncJournal.class);
	
	private LinkedList<AsyncJournalSegment<T>> segments = new LinkedList<>();
	private ReentrantLock segmentLock = new ReentrantLock();
	
	private Map<String, Metric> metrics;

	abstract protected AsyncWriter<T> getWriter();
	abstract protected AsyncReader<T> getReader();
	abstract protected AsyncJournalSegment<T> _newSegment() throws IOException;
	
	private AsyncJournalSegment<T> newSegment() throws IOException{
		LOG.debug("Creating new segment");
		AsyncJournalSegment<T> seg = this._newSegment();
		segments.add(seg);
		return seg;
	}
	
	public void removeSegment(AsyncJournalSegment<T> seg) throws IOException{
		segmentLock.lock();
		try {
			Iterator<AsyncJournalSegment<T>> iter = segments.iterator();
			AsyncJournalSegment<T> s;
			while(iter.hasNext()){
				s = iter.next();
				if(s.equals(seg)){
					if(s.canBeRemoved()){
						iter.remove();
						s.remove();
					}
					break;
				}
			}
		} finally {
			segmentLock.unlock();
		}
	}
	
	public AsyncJournalSegment<T> getFirst() throws IOException{
		segmentLock.lock();
		try {
			return segments.getFirst();
		} catch(NoSuchElementException e){
			LOG.debug("Getting new segment");
			return newSegment();
		} finally {
			segmentLock.unlock();
		}
	}
	public AsyncJournalSegment<T> getLast() throws IOException{
		segmentLock.lock();
		try {
			return segments.getLast();
		} catch(NoSuchElementException e){
			LOG.debug("Getting new segment");
			return newSegment();
		} finally {
			segmentLock.unlock();
		}
	}
	public AsyncJournalSegment<T> getNew() throws IOException{
		return newSegment();
	}
	
	protected void addSegment(AsyncJournalSegment<T> segment){
		segmentLock.lock();
		try {
			segments.add(segment);
		} finally {
			segmentLock.unlock();
		}
	}
	
	protected void addAllSegment(Collection<AsyncJournalSegment<T>> segments){
		segmentLock.lock();
		try {
			segments.addAll(segments);
		} finally {
			segmentLock.unlock();
		}
	}

	@Override
	public long totalMessages() {
		long total = 0;
		
		Iterator<AsyncJournalSegment<T>> iter = this.segments.iterator();
		AsyncJournalSegment<T> seg = null;
		while(iter.hasNext()){
			seg = iter.next();
			total += seg.msgCount();
		}
		return total+this.getReader().getQueuedMessages()+this.getWriter().getQueuedMessages();
	}

	@Override
	public long readMessages() {
		long total = 0;
		Iterator<AsyncJournalSegment<T>> iter = this.segments.iterator();
		AsyncJournalSegment<T> seg = null;
		while(iter.hasNext()){
			seg = iter.next();
			total += seg.msgReadCount();
		}
		return total-this.getReader().getQueuedMessages();
	}

	@Override
	public long unreadMessages() {
		long total = 0;
		long totalRead = 0;
		Iterator<AsyncJournalSegment<T>> iter = this.segments.iterator();
		AsyncJournalSegment<T> seg = null;
		while(iter.hasNext()){
			seg = iter.next();
			total += seg.msgCount();
			totalRead += seg.msgReadCount();
		}
		return total-totalRead+this.getReader().getQueuedMessages()+this.getWriter().getQueuedMessages();
	}
	

	@Override
	public int queuedWrites(){
		return AsyncJournal.this.getWriter().getQueuedMessages();
	}
	@Override
	public int queuedReads(){
		return AsyncJournal.this.getReader().getQueuedMessages();
	}
	

	@Override
	public boolean isWritingClosed(){
		return this.getWriter().isClosed();
	}
	@Override
	public boolean isReadingClosed(){
		return this.getReader().isClosed();
	}
	@Override
	public boolean isClosed(){
		return isWritingClosed() && isReadingClosed();
	}
	
	@Override
	public synchronized void open() throws IOException {
		segments.clear();
		this._open();
		
		LOG.debug("Starting Writer");
		this.getWriter().start();
		
		LOG.debug("Starting Reader");
		this.getReader().start();
	}
	
	protected abstract void _open() throws IOException;

	@Override
	public void close() throws IOException {
		this.getWriter().close();
		this.getReader().close();
		for(AsyncJournalSegment<T> seg: this.segments){
			seg.close();
		}
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		try {
			this.getWriter().close();
		} catch (IOException e) {
			LOG.error("Failed to close file writer");
		}
		this.getReader().close(timeout, unit);
		for(AsyncJournalSegment<T> seg: this.segments){
			try {
				seg.close();
			} catch (IOException e) {
				LOG.error("Failed to close segment {}", seg);
			}
		}
	}

	@Override
	public CompletableFuture<Void> write(final byte[] bytes){
		return this
				.write(bytes, 0, bytes.length);
	}
	@Override
	public CompletableFuture<Void> write(final byte[] bytes, int offset, int length){
		return this.getWriter()
			.write(bytes, offset, length);
	}
	@Override
	public CompletableFuture<Void> write(final ByteBuffer buffer){
		return this.getWriter()
			.write(buffer);
	}
	
	@Override
	public byte[] read() throws IOException, InterruptedException {
		return this.getReader().read();
	}
	@Override
	public byte[] read(final long timeout, final TimeUnit timeUnit) throws IOException, InterruptedException{
		return this.getReader().read(timeout, timeUnit);
	}
	@Override
	public List<byte[]> readN(int numOfMessages) throws IOException, InterruptedException {
		return this.getReader().readN(numOfMessages);
	}
	@Override
	public Collection<byte[]> drain() {
		return this.getReader().drain();
	}
	
	@Override
	public Map<String, Metric> getMetrics() {
		if(metrics == null){
			ImmutableMap.Builder<String, Metric> metBuilder = ImmutableMap.builder();
			
			for(Map.Entry<String, Metric> m: this.getWriter().getMetrics().entrySet()){
				metBuilder.put(MetricRegistry.name("writer", m.getKey()), m.getValue());
			}
			for(Map.Entry<String, Metric> m: this.getReader().getMetrics().entrySet()){
				metBuilder.put(MetricRegistry.name("reader", m.getKey()), m.getValue());
			}
			
			metBuilder.put("UnreadMessages", new CachedGauge<Long>(100, TimeUnit.MILLISECONDS){
				@Override
				protected Long loadValue() {
					return AsyncJournal.this.unreadMessages();
				}
			});
			metBuilder.put("TotalMessages", new CachedGauge<Long>(100, TimeUnit.MILLISECONDS){
				@Override
				protected Long loadValue() {
					return AsyncJournal.this.totalMessages();
				}
			});
			metBuilder.put("ReadMessages", new CachedGauge<Long>(100, TimeUnit.MILLISECONDS){
				@Override
				protected Long loadValue() {
					return AsyncJournal.this.readMessages();
				}
			});
			metBuilder.put("TotalJournalSegments", new CachedGauge<Integer>(100, TimeUnit.MILLISECONDS){
				@Override
				protected Integer loadValue() {
					return AsyncJournal.this.segments.size();
				}
			});
			
			metrics = metBuilder.build();
		}
		return metrics;
	}

	@Override
	public Map<String, HealthCheck> getHealthChecksWarning() {
		ImmutableMap.Builder<String, HealthCheck> healthBuilder = ImmutableMap.builder();
		healthBuilder.putAll(this.getReader().getHealthChecksWarning());
		healthBuilder.putAll(this.getWriter().getHealthChecksWarning());
		return healthBuilder.build();
	}

	@Override
	public Map<String, HealthCheck> getHealthChecksError() {
		ImmutableMap.Builder<String, HealthCheck> healthBuilder = ImmutableMap.builder();
		healthBuilder.putAll(this.getReader().getHealthChecksError());
		healthBuilder.putAll(this.getWriter().getHealthChecksError());
		return healthBuilder.build();
	}
}
