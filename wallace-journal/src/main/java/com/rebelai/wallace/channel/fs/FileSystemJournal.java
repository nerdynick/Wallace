package com.rebelai.wallace.channel.fs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.rebelai.wallace.channel.AsyncJournal;
import com.rebelai.wallace.channel.AsyncJournalSegment;
import com.rebelai.wallace.channel.AsyncReader;
import com.rebelai.wallace.channel.AsyncWriter;

public class FileSystemJournal extends AsyncJournal<AsynchronousFileChannel> {
	private static final Logger LOG = LoggerFactory.getLogger(FileSystemJournal.class);
	
	private static final String extention = ".jrnl";
	
	final Path journalPath;
	protected final long maxSegmentSize;
	protected final int maxWriteQueueSize;
	private final AsyncFSWriter journalWriter;
	private final AsyncFSReader journalReader;
	
	protected FilenameFilter journalNameFilter = new FilenameFilter() {
        @Override public boolean accept(File dir, String name) {
            return name.endsWith(FileSystemJournal.extention);
        }
    };
	
	public FileSystemJournal(final Path journalDirectoryPath) throws IOException{
		//Defaults to 1GB
		this(journalDirectoryPath, 1024*1024*1000, 1000, 100);
	}
	
	public FileSystemJournal(final Path journalDirectoryPath, final long maxSegmentSize, final int maxWriteQueueSize, final int maxReadBufferSize) throws IOException{
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
        
        this.journalWriter = new AsyncFSWriter(this, maxWriteQueueSize, maxSegmentSize);
        this.journalReader = new AsyncFSReader(this, maxReadBufferSize);
        
        
	}

	@Override
	protected AsyncJournalSegment<AsynchronousFileChannel> _newSegment() throws IOException {
		return new AsyncFSSegment(newJournal());
	}
	
	protected Path newJournal(){
		return journalPath.resolve(Long.toString(System.currentTimeMillis())+FileSystemJournal.extention);
	}

	@Override
	public synchronized void open() throws IOException {
		LOG.debug("Searching for journals");
		final TreeSet<String> journals = getAllJournals();
		LOG.debug("Found {} journals", journals.size());
		for(String j: journals){
			final Path p = Paths.get(j);
			final AsyncFSSegment seg = new AsyncFSSegment(p);
			this.addSegment(seg);
		}
		
		LOG.debug("Starting Writer");
		journalWriter.start();
		
		LOG.debug("Starting Reader");
		journalReader.start();
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
	
	public boolean isClosed(){
		return this.journalWriter.isClosed() && this.journalReader.isClosed();
	}

	@Override
	public void close() throws IOException {
		this.journalWriter.close();
		this.journalReader.close();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		try {
			this.journalWriter.close();
		} catch (IOException e) {
			LOG.error("Failed to close file writer");
		}
		this.journalReader.close(timeout, unit);
	}
	
	public CompletableFuture<Void> write(final byte[] bytes){
		return this.write(bytes, 0, bytes.length);
	}
	public CompletableFuture<Void> write(final byte[] bytes, int offset, int length){
		return this.journalWriter
			.write(bytes, offset, length);
	}
	public CompletableFuture<Void> write(final ByteBuffer buffer){
		return this.journalWriter
			.write(buffer);
	}

	@Override
	public byte[] read() throws IOException, InterruptedException {
		return journalReader.read();
	}
	
	@Override
	public byte[] read(final long timeout, final TimeUnit timeUnit) throws IOException, InterruptedException{
		return journalReader.read(timeout, timeUnit);
	}

	@Override
	public List<byte[]> readN(int numOfMessages) throws IOException, InterruptedException {
		return journalReader.readN(numOfMessages);
	}

	@Override
	protected AsyncWriter<AsynchronousFileChannel> getWriter() {
		return this.journalWriter;
	}

	@Override
	protected AsyncReader<AsynchronousFileChannel> getReader() {
		return this.journalReader;
	}

}
