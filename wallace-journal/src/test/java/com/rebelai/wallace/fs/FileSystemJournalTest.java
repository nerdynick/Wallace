package com.rebelai.wallace.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

public class FileSystemJournalTest {
	@Test
	public void testWriting() throws IOException{
		Path journalDir = Files.createTempDirectory("WallaceTemp");
		FileSystemJournal journal = new FileSystemJournal(journalDir, 12, 5, 5);
		
		assertFalse(journal.isClosed());
		
		ByteBuffer buffers = ByteBuffer.allocate(12);
		buffers.putInt(1);
		buffers.putInt(2);
		buffers.putInt(3);
		
		buffers.flip();
		journal.write(buffers);
		
		assertNotNull(journal.getEarliestJournal());
		assertNotNull(journal.getLatestJournal());
		assertEquals(journal.getEarliestJournal(), journal.getLatestJournal());
		
		buffers.flip();
		journal.write(buffers);
		assertNotEquals(journal.getEarliestJournal(), journal.getLatestJournal());
		
		journal.close();
	}
}
