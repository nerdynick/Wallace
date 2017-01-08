package com.rebelai.wallace;

import java.nio.ByteBuffer;

import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.eclipse.jetty.io.ByteBufferPool;

public class OversizedArrayByteBufferPool implements ByteBufferPool {
	private final ArrayByteBufferPool pool;
	private final int maxSize;

	public OversizedArrayByteBufferPool() {
		this(-1,-1,-1,-1);
	}

	public OversizedArrayByteBufferPool(int minSize, int increment, int maxSize, int maxQueue) {
		if(maxSize <= 0){
			maxSize=64*1024;
		}
		this.maxSize = maxSize;
		pool = new ArrayByteBufferPool(minSize, increment, maxSize, maxQueue);
	}

	public OversizedArrayByteBufferPool(int minSize, int increment, int maxSize) {
		 this(minSize,increment,maxSize,-1);
	}

	@Override
	public ByteBuffer acquire(int size, boolean direct) {
		if(size > maxSize){
			if(direct){
				ByteBuffer b = ByteBuffer.allocateDirect(size);
				b.clear();
				return b;
			} else {
				ByteBuffer b = ByteBuffer.allocate(size);
				b.clear();
				return b;
			}
		}
		return pool.acquire(size, direct);
	}

	@Override
	public void release(ByteBuffer buffer) {
		buffer.clear();
		if(buffer.capacity() <= maxSize){
			pool.release(buffer);
		}
	}
}
