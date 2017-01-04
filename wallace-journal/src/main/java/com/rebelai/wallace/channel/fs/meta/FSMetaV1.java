package com.rebelai.wallace.channel.fs.meta;

import java.nio.MappedByteBuffer;

public class FSMetaV1 extends FSMeta {
	private static final int OFFSET_Start = 0;
	private static final int OFFSET_Length = 8;
	
	private static final int READ_Start = OFFSET_Start+OFFSET_Length;
	private static final int READ_Length = 4;
	
	private static final int TOTAL_Start = READ_Start+READ_Length;
	private static final int TOTAL_Length = 4;
	
	private static final int REGION_LENGTH = OFFSET_Length+READ_Length+TOTAL_Length;

	@Override
	public void init(final MappedByteBuffer buffer){
		readOffset(buffer, 0l);
		msgReadCount(buffer, 0);
		msgCount(buffer, 0);
	}

	@Override
	public int regionLength() {
		return REGION_LENGTH;
	}

	@Override
	public long readOffset(MappedByteBuffer buffer) {
		return buffer.getLong(OFFSET_Start);
	}
	@Override
	public long readOffset(MappedByteBuffer buffer, long value) {
		buffer.putLong(OFFSET_Start, value);
		return value;
	}

	@Override
	public int msgReadCount(MappedByteBuffer buffer) {
		return buffer.getInt(READ_Start);
	}

	@Override
	public int msgReadCount(MappedByteBuffer buffer, int value) {
		buffer.putInt(READ_Start, value);
		return value;
	}

	@Override
	public int msgCount(MappedByteBuffer buffer) {
		return buffer.getInt(TOTAL_Start);
	}

	@Override
	public int msgCount(MappedByteBuffer buffer, int value) {
		buffer.putInt(TOTAL_Start, value);
		return value;
	}

}
