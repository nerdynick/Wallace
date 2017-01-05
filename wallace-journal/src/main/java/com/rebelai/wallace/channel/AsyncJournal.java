package com.rebelai.wallace.channel;

import java.io.IOException;
import java.nio.channels.AsynchronousChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
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
	

	public int queuedWrites(){
		return AsyncJournal.this.getWriter().getQueuedMessages();
	}
	public int queuedReads(){
		return AsyncJournal.this.getReader().getQueuedMessages();
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
		healthBuilder.put("ReaderBufferCapacity", new HealthCheck(){
			@Override
			protected Result check() throws Exception {
				if(AsyncJournal.this.getReader().getQueuedMessagePercentFull() > 75){
					return Result.unhealthy("Reader buffer is > 75% full");
				}
				return Result.healthy();
			}
		});
		healthBuilder.put("WriteBufferCapacity", new HealthCheck(){
			@Override
			protected Result check() throws Exception {
				if(AsyncJournal.this.getWriter().getQueuedMessagePercentFull() > 75){
					return Result.unhealthy("Write buffer is > 75% full");
				}
				return Result.healthy();
			}
		});
		return healthBuilder.build();
	}

	@Override
	public Map<String, HealthCheck> getHealthChecksError() {
		ImmutableMap.Builder<String, HealthCheck> healthBuilder = ImmutableMap.builder();
		
		healthBuilder.put("ReaderStopped", new HealthCheck(){
			@Override
			protected Result check() throws Exception {
				if(AsyncJournal.this.getReader().isClosed()){
					return Result.unhealthy("Reader has been stopped or closed");
				}
				return Result.healthy();
			}
		});
		healthBuilder.put("WritingStopped", new HealthCheck(){
			@Override
			protected Result check() throws Exception {
				if(AsyncJournal.this.getWriter().isClosed()){
					return Result.unhealthy("Writer has been stopped or closed");
				}
				return Result.healthy();
			}
		});
		
		return healthBuilder.build();
	}
}
