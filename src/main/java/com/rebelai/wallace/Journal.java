package com.rebelai.wallace;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public interface Journal extends Closeable {
	public boolean isClosed();
	public void write(final byte[] bytes) throws IOException;
	public void write(final byte[] bytes, int offset, int length) throws IOException;
	public void write(final ByteBuffer buffer) throws IOException;
	
	public byte[] read() throws IOException, InterruptedException;
	public List<byte[]> readN(final int numOfMessages) throws IOException, InterruptedException;
}
