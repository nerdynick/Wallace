package com.rebelai.wallace.channel.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemJournalTest {
	private static final Logger LOG = LoggerFactory.getLogger(FileSystemJournalTest.class);
	
//	@Test
	public void testWriteRead() throws IOException, InterruptedException, ExecutionException, TimeoutException{
		LOG.debug("Doing Write/Read test");
		final int msgCount = 10; //Must be an even number
		final Path journalDir = Files.createTempDirectory("WallaceTemp");
		final ByteBuffer buffers = ByteBuffer.allocate(12);
		buffers.putChar('a');
		buffers.putChar('b');
		buffers.putChar('c');
		buffers.putChar('d');
		buffers.putChar('e');
		buffers.putChar('f');
		buffers.flip();
		
		//NOTE: Max segment size is sized less then the total number of messages 
		//	intended length would actually be. To test that full messages are 
		//	written to segments before rolling.
		final long maxSegmentSize = ((msgCount/2)*(buffers.limit()+4))-4; 
		final int maxWriteQueueSize = msgCount+5; 
		final int maxReadBufferSize = msgCount+5; 
		final int maxMessageSize = 1024*2;
		final int readBufferSize = buffers.limit()/2;
		FileSystemJournal journal = new FileSystemJournal(journalDir, maxSegmentSize, maxWriteQueueSize, maxReadBufferSize, maxMessageSize, readBufferSize);
		LOG.debug("Opening");
		journal.open();
		
		assertFalse(journal.isClosed());
		assertEquals(journal.getFirst(), journal.getLast());
		assertEquals(0, journal.totalMessages());
		assertEquals(0, journal.queuedWrites());
		assertEquals(0, journal.queuedReads());
		
		LOG.debug("Writing 1st set");
		for(int i=0; i<msgCount/2; i++){
			LOG.debug("Write");
			buffers.position(0);
			journal.write(buffers);
		}
		
		LOG.debug("Waiting for write to be done");
		journal.getWriter().await(5, TimeUnit.SECONDS);
		assertEquals(msgCount/2, journal.totalMessages());
		
		//Ensure the journal rolled
		assertNotNull(journal.getFirst());
		assertNotNull(journal.getLast());
		assertNotEquals(journal.getFirst(), journal.getLast());
		
		LOG.debug("Writing 2nd set");
		for(int i=0; i<msgCount/2; i++){
			buffers.position(0);
			journal.write(buffers);
		}
		LOG.debug("Waiting for write to be done");
		journal.getWriter().await(5, TimeUnit.SECONDS);
		LOG.debug("All writing is done. Waiting for reads");
		journal.getReader().await(5, TimeUnit.SECONDS);
		
		assertEquals(msgCount, journal.totalMessages());
		assertEquals(0, journal.queuedWrites());
		assertEquals(msgCount, journal.queuedReads());
		assertEquals(journal.getReader().currentFileOffset.get(), journal.getReader().currentSegment().readOffset());
		
		LOG.debug("Validating rolls");
		//We should have rolled both readers and writers and they should now be on the same file
		assertEquals(journal.getFirst(), journal.getLast()); 
		assertTrue(journal.getLast().isOpenForReading());
		assertTrue(journal.getFirst().isOpenForWriting());
		
		LOG.debug("Closing journal.");
		//We should be able to close the journal at this point
		//All the reads from disk should be done.
		//And we should be able to read all the buffered messages to support draining
		journal.close();
		
		
		LOG.debug("Reading");
		List<byte[]> msgs = new ArrayList<>(msgCount);
		for(int i = 0; i<msgCount; i++){
			LOG.debug("Getting MSG: {}", i);
			//We do not wait for reads here. This validates the isReading flags are working right as per the await() above
			msgs.add(journal.read());
		}
		//We should have been able to get both messages we wrote
		assertEquals(msgCount, msgs.size());
		LOG.debug("Messages Read: {}", msgs);
		
		ByteBuffer msgBuffer = ByteBuffer.allocate(12);
		for(byte[] b: msgs){
			assertNotNull(b);
			assertEquals(buffers.limit(), b.length);
			msgBuffer.clear();
			msgBuffer.put(b);
			msgBuffer.flip();
			assertEquals('a', msgBuffer.getChar());
			assertEquals('b', msgBuffer.getChar());
			assertEquals('c', msgBuffer.getChar());
		}
	}
	
	@Test
	public void testWriteReadCloseOpen() throws IOException, InterruptedException, ExecutionException, TimeoutException{
		LOG.info("Doing Write/Read Open/Close test");
		final int msgCount = 10; //Must be an even number
		final Path journalDir = Files.createTempDirectory("WallaceTemp");
		final ByteBuffer buffers = ByteBuffer.allocate(12);
		buffers.putChar('a');
		buffers.putChar('b');
		buffers.putChar('c');
		buffers.putChar('d');
		buffers.putChar('e');
		buffers.putChar('f');
		buffers.flip();
		
		final long maxSegmentSize = ((msgCount+1)*(buffers.limit()+4))-4; 
		final int maxWriteQueueSize = msgCount+5; 
		final int maxReadBufferSize = msgCount+5; 
		final int maxMessageSize = 1024*2;
		final int readBufferSize = buffers.limit()/2;
		FileSystemJournal journal = new FileSystemJournal(journalDir, maxSegmentSize, maxWriteQueueSize, maxReadBufferSize, maxMessageSize, readBufferSize);
		LOG.debug("Opening");
		journal.open();
		
		assertFalse(journal.isClosed());
		assertEquals(journal.getFirst(), journal.getLast());
		assertEquals(0, journal.totalMessages());
		assertEquals(0, journal.queuedWrites());
		assertEquals(0, journal.queuedReads());
		
		LOG.debug("Writing 1st message");
		buffers.position(0);
		journal.write(buffers);
		LOG.debug("Waiting for write to be done");
		journal.getWriter().await(5, TimeUnit.SECONDS);
		LOG.debug("All writing is done. Waiting for reads");
		assertEquals(1, journal.totalMessages());
		journal.getReader().await(5, TimeUnit.SECONDS);
		
		final long s = System.currentTimeMillis();
		while(TimeUnit.SECONDS.toMillis(5) > (System.currentTimeMillis()-s)){
			if(journal.queuedReads() >= 1){
				break;
			}
		}
		assertEquals(1, journal.queuedReads());

		assertEquals(journal.getReader().currentFileOffset.get(), journal.getReader().currentSegment().readOffset());
		LOG.debug("Stopping reader to write other messages");
		journal.getReader().close();
		
		LOG.debug("Writing 1st set");
		for(int i=0; i<msgCount/2; i++){
			buffers.position(0);
			journal.write(buffers);
		}
		LOG.debug("Waiting for write to be done");
		journal.getWriter().await(5, TimeUnit.SECONDS);
		
		//Ensure the journal rolled
		journal.close();
		journal.open();
		assertEquals(journal.getReader().currentFileOffset.get(), journal.getReader().currentSegment().readOffset());
		
		LOG.debug("Writing 2nd set");
		for(int i=0; i<msgCount/2; i++){
			buffers.position(0);
			journal.write(buffers);
		}
		LOG.debug("Waiting for write to be done");
		journal.getWriter().await(5, TimeUnit.SECONDS);
		LOG.debug("All writing is done. Waiting for reads");
		journal.getReader().await(5, TimeUnit.SECONDS);
		
		assertEquals(msgCount+1, journal.totalMessages());
		assertEquals(0, journal.queuedWrites());
		assertEquals(msgCount+1, journal.queuedReads());
		
		LOG.debug("Closing journal.");
		//We should be able to close the journal at this point
		//All the reads from disk should be done.
		//And we should be able to read all the buffered messages to support draining
		journal.close();
		
		
		LOG.debug("Reading");
		List<byte[]> msgs = new ArrayList<>(msgCount);
		for(int i = 0; i<msgCount+1; i++){
			LOG.debug("Getting MSG: {}", i);
			//We do not wait for reads here. This validates the isReading flags are working right as per the await() above
			msgs.add(journal.read());
		}
		//We should have been able to get both messages we wrote
		assertEquals(msgCount+1, msgs.size());
		LOG.debug("Messages Read: {}", msgs);
		
		ByteBuffer msgBuffer = ByteBuffer.allocate(12);
		for(byte[] b: msgs){
			assertNotNull(b);
			assertEquals(buffers.limit(), b.length);
			msgBuffer.clear();
			msgBuffer.put(b);
			msgBuffer.flip();
			assertEquals('a', msgBuffer.getChar());
			assertEquals('b', msgBuffer.getChar());
			assertEquals('c', msgBuffer.getChar());
			assertEquals('d', msgBuffer.getChar());
			assertEquals('e', msgBuffer.getChar());
			assertEquals('f', msgBuffer.getChar());
		}
	}
}
