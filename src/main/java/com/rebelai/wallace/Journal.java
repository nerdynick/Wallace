package com.rebelai.wallace;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface Journal extends Closeable {
	public boolean isClosed();
	public void write(final byte[] bytes) throws IOException;
	public void write(final byte[] bytes, int offset, int length) throws IOException;
	public ByteBuffer write(final ByteBuffer buffer) throws IOException;
	
	public byte[] read() throws IOException, InterruptedException;
	public byte[] read(final long timeout, final TimeUnit timeUnit) throws IOException, InterruptedException;
	/**
	 * Fetch N number of messages from the buffer. This will block and wait for new messages to be buffered.
	 * If the journal is closed and the buffer contains messages. Those messages will be drained into the return. 
	 * This can result in more messages being returned then originally asked for. The intent of this is to allow post close
	 * the ability to drain the buffer, reducing the chance of lost messages.
	 * 
	 * @param numOfMessages
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public List<byte[]> readN(final int numOfMessages) throws IOException, InterruptedException;
}
