package com.rebelai.wallace.channel.fs.meta;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

public abstract class FSMeta {
	private static final Map<Integer, FSMeta> metaMaps;
	private static final int latestMap;
	static {
		metaMaps = new HashMap<>();
		metaMaps.put(1, new FSMetaV1());
		
		latestMap = 1;
	}
	public abstract int regionLength();
	
	public abstract void init(final MappedByteBuffer buffer);
	
	public abstract long readOffset(final MappedByteBuffer buffer);
	public abstract long readOffset(final MappedByteBuffer buffer, final long value);
	
	public abstract int msgReadCount(final MappedByteBuffer buffer);
	public abstract int msgReadCount(final MappedByteBuffer buffer, final int value);
	
	public abstract int msgCount(final MappedByteBuffer buffer);
	public abstract int msgCount(final MappedByteBuffer buffer, final int value);
	
	public static FSMeta getMetaMapper(final int version) throws IOException{
		return getMetaMapper(Integer.valueOf(version));
	}
	public static FSMeta getMetaMapper(final Integer version) throws IOException{
		final FSMeta meta = metaMaps.get(Preconditions.checkNotNull(version));
		if(meta == null){
				throw new IOException("Journal MetaIndex is invalid. Meta map doesn't exist.");
		}
		return meta;
	}
	public static int getLatestVersion(){
		return latestMap;
	}
}
