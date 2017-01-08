package com.rebelai.wallace.channel.fs.meta;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import com.rebelai.wallace.JournalSegment;
import com.rebelai.wallace.channel.fs.AsyncFSSegment;

public class AsyncFSSegmentTest {

	@Test
	public void testMetaIndexDefault() throws IOException {
		Path segmentPath = Files.createTempFile("WallaceTest", JournalSegment.extention);
		AsyncFSSegment segment = new AsyncFSSegment(segmentPath);
		segment.readOffset(10);
		segment.msgReadCount(10);
		segment.msgCount(20);
		assertEquals(10, segment.readOffset());
		assertEquals(10, segment.msgReadCount());
		assertEquals(20, segment.msgCount());
		
		segment.readOffsetIncr(1);
		segment.msgReadCountIncr();
		segment.msgCountIncr();
		assertEquals(11, segment.readOffset());
		assertEquals(11, segment.msgReadCount());
		assertEquals(21, segment.msgCount());
		
		segment.close();
		segment = new AsyncFSSegment(segmentPath);
		assertEquals(11, segment.readOffset());
		assertEquals(11, segment.msgReadCount());
		assertEquals(21, segment.msgCount());
		segment.close();
		
	}

}
