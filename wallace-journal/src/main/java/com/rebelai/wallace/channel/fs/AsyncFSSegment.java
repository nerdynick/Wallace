package com.rebelai.wallace.channel.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.rebelai.wallace.JournalSegment;
import com.rebelai.wallace.JournalSegmentStats;
import com.rebelai.wallace.channel.AsyncJournalSegment;
import com.rebelai.wallace.channel.fs.meta.FSMeta;

public class AsyncFSSegment implements AsyncJournalSegment<AsynchronousFileChannel> {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncFSSegment.class);
	
	
	
	private final AtomicLong readOffset;
	private final AtomicInteger msgRead;
	private final AtomicInteger msgTotal;

	private final ReentrantReadWriteLock readLock = new ReentrantReadWriteLock();
	private final ReentrantReadWriteLock writeLock = new ReentrantReadWriteLock();
	private AsynchronousFileChannel readChannel;
	private AsynchronousFileChannel writeChannel;
	
	private final RandomAccessFile metaIndex;
	private final FSMeta metaMap;
	private final MappedByteBuffer mapBuffer;
	
	private final Path segment;
	private final Path segmentMeta;
	
	public AsyncFSSegment(Path segment) throws IOException{
		this.segment = Preconditions.checkNotNull(segment);
		if(!Files.exists(segment)){
			LOG.info("Creating new Segment at {}", segment);
			Files.createFile(segment);
		} else {
			LOG.info("Loading Segment at {}", segment);
		}
		
		boolean shouldInitMeta = false;
		segmentMeta = Paths.get(segment.toString()+metaIndexExt);
		if(!Files.exists(segmentMeta)){
			LOG.debug("Creating new Segment MetaIndex");
			Files.createFile(segmentMeta);
			shouldInitMeta = true;
		}
		
		metaIndex = new RandomAccessFile(segmentMeta.toFile(), "rw");
		FileChannel channel = metaIndex.getChannel();
		ByteBuffer versionBuffer = ByteBuffer.allocate(4);
		if(shouldInitMeta){
			versionBuffer.putInt(0, FSMeta.getLatestVersion());
			channel.write(versionBuffer);
			metaMap = FSMeta.getMetaMapper(versionBuffer.getInt(0));
		} else {
			channel.read(versionBuffer);
			metaMap = FSMeta.getMetaMapper(versionBuffer.getInt(0));
		}
		versionBuffer.clear();
		mapBuffer = channel.map(MapMode.READ_WRITE, 4, metaMap.regionLength());
		
		if(shouldInitMeta){
			metaMap.init(mapBuffer);
		}
		readOffset = new AtomicLong(metaMap.readOffset(mapBuffer));
		msgRead = new AtomicInteger(metaMap.msgReadCount(mapBuffer));
		msgTotal = new AtomicInteger(metaMap.msgCount(mapBuffer));
		
		LOG.info("Segment Loaded with Info: Offset={}, ReadMsgs={}, TotalMsgs={} for segment {}", readOffset.get(), msgRead.get(), msgTotal.get(), this);
	}

	@Override
	public String toString(){
		StringBuilder b = new StringBuilder()
			.append("FSSegment(")
			.append(this.segment)
			.append(" IsRead: ").append(this.readChannel != null)
			.append(" IsWrite: ").append(this.writeChannel != null)
			.append(")");
		return b.toString();
	}

	@Override
	public JournalSegmentStats getStats(){
		return new JournalSegmentStats(readOffset(), msgReadCount(), msgCount(), isOpenForReading(), isOpenForWriting());
	}
	
	@Override
	public long readOffset() {
		return readOffset.get();
	}

	@Override
	public void readOffset(long value) {
		readOffset.set(value);
		metaMap.readOffset(mapBuffer, value);
	}

	@Override
	public void readOffsetIncr(int value) {
		metaMap.readOffset(mapBuffer, readOffset.addAndGet(value));
	}

	@Override
	public int msgReadCount() {
		return msgRead.get();
	}

	@Override
	public void msgReadCount(int value) {
		msgRead.set(value);
		metaMap.msgReadCount(mapBuffer, value);
	}

	@Override
	public int msgReadCountIncr() {
		return metaMap.msgReadCount(mapBuffer, msgRead.incrementAndGet());
	}

	@Override
	public int msgCount() {
		return msgTotal.get();
	}

	@Override
	public void msgCount(int value) {
		msgTotal.set(value);
		metaMap.msgCount(mapBuffer, value);
	}

	@Override
	public int msgCountIncr() {
		return metaMap.msgCount(mapBuffer, msgTotal.incrementAndGet());
	}

	@Override
	public boolean isOpenForReading() {
		readLock.readLock().lock();
		try {
			return readChannel != null && readChannel.isOpen();
		} finally {
			readLock.readLock().unlock();
		}
	}

	@Override
	public boolean isOpenForWriting() {
		writeLock.readLock().lock();
		try {
			return writeChannel != null && writeChannel.isOpen();
		} finally {
			writeLock.readLock().unlock();
		}
	}

	@Override
	public boolean canBeRemoved() {
		return !this.isOpenForReading() && !this.isOpenForWriting();
	}

	@Override
	public void closeRead() throws IOException {
		readLock.writeLock().lock();
		try {
			if(readChannel != null){
				readChannel.close();
				readChannel = null;
			}
		} finally {
			readLock.writeLock().unlock();
		}
	}

	@Override
	public void closeWrite() throws IOException {
		writeLock.writeLock().lock();
		try {
			if(writeChannel != null){
				writeChannel.close();
				writeChannel = null;
			}
		} finally {
			writeLock.writeLock().unlock();
		}
	}

	@Override
	public void close() throws IOException {
		closeRead();
		closeWrite();
		mapBuffer.force();
		metaIndex.close();
	}

	@Override
	public boolean remove() throws IOException {
		if(this.canBeRemoved()){
			readLock.writeLock().lock();
			writeLock.writeLock().lock();
			try {
				LOG.debug("Closing MetaIndex");
				mapBuffer.force();
				metaIndex.close();
				LOG.debug("Deleting Segment and MetaIndex. Seg: {} Meta: {}", segment, segmentMeta);
				Files.delete(segment);
				Files.delete(segmentMeta);
				LOG.debug("Done");
			} finally {
				readLock.writeLock().unlock();
				writeLock.writeLock().unlock();
			}
		} else {
			LOG.warn("Attempted to close before read/write channels where closed. For {}", this);
		}
		return false;
	}

	@Override
	public AsynchronousFileChannel openRead() throws IOException {
		readLock.writeLock().lock();
		try {
			if(readChannel == null || !readChannel.isOpen()){
				readChannel = AsynchronousFileChannel.open(segment, StandardOpenOption.READ);
			}
		} finally {
			readLock.writeLock().unlock();
		}
		return readChannel;
	}

	@Override
	public AsynchronousFileChannel openWrite() throws IOException {
		writeLock.writeLock().lock();
		try {
			if(writeChannel == null || !writeChannel.isOpen()){
				writeChannel = AsynchronousFileChannel.open(segment, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			}
		} finally {
			writeLock.writeLock().unlock();
		}
		return writeChannel;
	}

	@Override
	public boolean equals(JournalSegment<AsynchronousFileChannel> seg) {
		if(seg instanceof AsyncFSSegment){
			if(this.segment.equals(((AsyncFSSegment) seg).segment)){
				return true;
			}
		}
		
		return false;
	}

}
