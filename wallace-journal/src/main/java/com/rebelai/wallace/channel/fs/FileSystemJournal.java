package com.rebelai.wallace.channel.fs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.rebelai.wallace.JournalSegment;
import com.rebelai.wallace.channel.AsyncJournal;
import com.rebelai.wallace.channel.AsyncJournalSegment;

public class FileSystemJournal extends AsyncJournal<AsynchronousFileChannel> {
	private static final Logger LOG = LoggerFactory.getLogger(FileSystemJournal.class);
	
	final Path journalPath;
	protected final long maxSegmentSize;
	protected final int maxWriteQueueSize;
	private final AsyncFSWriter journalWriter;
	private final AsyncFSReader journalReader;
	
	protected FilenameFilter journalNameFilter = new FilenameFilter() {
        @Override public boolean accept(File dir, String name) {
            return name.endsWith(JournalSegment.extention);
        }
    };
	
	public FileSystemJournal(final Path journalDirectoryPath, final long maxSegmentSize, final int maxWriteQueueSize, final int maxReadBufferSize, final int maxMessageSize, final int readBufferSize) throws IOException{
		super();
		this.journalPath = journalDirectoryPath;
		this.maxSegmentSize = maxSegmentSize;
		this.maxWriteQueueSize = maxWriteQueueSize;
		
		if (!Files.exists(journalDirectoryPath)) {
            throw new IOException(journalDirectoryPath + " directory doesn't exist.");            
        }
        
        if (!Files.isDirectory(journalDirectoryPath)) {
            throw new IOException(journalDirectoryPath + " is not directory.");
        }

        if (!Files.isWritable(journalDirectoryPath)) {
            throw new IOException(journalDirectoryPath + " directory is not writeable.");
        }
        
        LOG.info("Creating journal at {}", journalDirectoryPath);
        
        this.journalWriter = new AsyncFSWriter(this, maxWriteQueueSize, maxSegmentSize, maxMessageSize);
        this.journalReader = new AsyncFSReader(this, maxReadBufferSize, maxMessageSize, readBufferSize);
	}

	@Override
	protected AsyncJournalSegment<AsynchronousFileChannel> _newSegment() throws IOException {
		return new AsyncFSSegment(newJournal());
	}
	
	protected Path newJournal(){
		return journalPath.resolve(Long.toString(System.currentTimeMillis())+JournalSegment.extention);
	}

	@Override
	protected void _open() throws IOException {
		LOG.debug("Searching for journals");
		final TreeSet<String> journals = getAllJournals();
		LOG.debug("Found {} journals", journals.size());
		for(String j: journals){
			final Path p = journalPath.resolve(j);
			final AsyncFSSegment seg = new AsyncFSSegment(p);
			this.addSegment(seg);
		}
	}
	
	private TreeSet<String> getAllJournals(){
		final String[] journalPaths = journalPath.toFile().list(journalNameFilter);
		
		final TreeSet<String> journals = new TreeSet<>();
		if(journalPaths != null){
			for(String j: journalPaths){
				if(!Strings.isNullOrEmpty(j)){
					journals.add(j);
				}
			}
		}
		
		return journals;
	}

	@Override
	protected AsyncFSWriter getWriter() {
		return this.journalWriter;
	}

	@Override
	protected AsyncFSReader getReader() {
		return this.journalReader;
	}

}
