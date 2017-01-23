package com.rebelai.wallace;

import java.io.Closeable;
import java.io.IOException;

public interface JournalSegment<T> extends Closeable {
	public static final String extention = ".jrnl";
	public static final String metaIndexExt = ".idx";
	
	public long readOffset();
	public void readOffset(long value);
	public void readOffsetIncr(int value);
	
	public int msgReadCount();
	public void msgReadCount(int value);
	public int msgReadCountIncr();
	
	public int msgCount();
	public void msgCount(int value);
	public int msgCountIncr();
	
	public boolean isOpenForReading();
	public boolean isOpenForWriting();
	public boolean canBeRemoved();
	
	public void closeRead() throws IOException;
	public void closeWrite() throws IOException;
	public boolean remove() throws IOException;
	
	public T openRead() throws IOException;
	public T openWrite() throws IOException;
	public boolean equals(JournalSegment<T> seg);
	
	public JournalSegmentStats getStats();
}
