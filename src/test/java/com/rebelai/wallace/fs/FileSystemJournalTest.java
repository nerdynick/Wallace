package com.rebelai.wallace.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import junit.framework.TestCase;

public class FileSystemJournalTest extends TestCase {
	@Test
	public void testWriting() throws IOException{
		Path journalDir = Files.createTempDirectory("WallaceTemp");
		FileSystemJournal journal = new FileSystemJournal(journalDir, 15, 5);
		
		assertFalse(journal.isClosed());
		
		ByteBuffer buffers = ByteBuffer.allocate(12);
		buffers.putInt(1);
		buffers.putInt(2);
		buffers.putInt(3);
		
		journal.write(buffers);
		
		assertNotNull(journal.getEarliestJournal());
		assertNotNull(journal.getLatestJournal());
		assertEquals(journal.getEarliestJournal(), journal.getLatestJournal());
	}
}
