package com.rebelai.wallace.fs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.eclipse.jetty.io.ByteBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.rebelai.wallace.Journal;

public class FileSystemJournal implements Journal {
	private static final Logger LOG = LoggerFactory.getLogger(FileSystemJournal.class);
	
	private static final String extention = ".jrnl";
	private static final int headerByteSize = 4; //We only store the length as long in the headers
	protected static final int maxMessageSize = 1000000; 
	
	final Path journalPath;
	protected final long maxSegmentSize;
	protected final int maxWriteQueueSize;
	private final Writer journalWriter;
	private final Reader journalReader;
	protected ByteBufferPool bufferPool;
	
	public FileSystemJournal(final Path journalDirectoryPath) throws IOException{
		//Defaults to 1GB
		this(journalDirectoryPath, 1024*1024*1000, 10000, 100);
	}
	
	public FileSystemJournal(final Path journalDirectoryPath, final long maxSegmentSize, final int maxWriteQueueSize, final int maxReadBufferSize) throws IOException{
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
        
        this.journalWriter = new Writer(this, maxWriteQueueSize, maxSegmentSize);
        this.journalReader = new Reader(this, maxReadBufferSize);
        
        bufferPool = new ArrayByteBufferPool(0, 1024, maxMessageSize);
	}
	
	public Path getEarliestJournal(){
		final TreeSet<String> journals = this.getAllJournals();
		if(!Iterables.isEmpty(journals)){
			return journalPath.resolve(journals.first());
		}
		
		return null;
	}
	
	public Path getLatestJournal(){
		final TreeSet<String> journals = this.getAllJournals();
		if(!Iterables.isEmpty(journals)){
			return journalPath.resolve(journals.last());
		}
		
		return null;
	}
	
	protected Path newJournal(){
		return journalPath.resolve(Long.toString(System.currentTimeMillis())+FileSystemJournal.extention);
	}
	
	public TreeSet<String> getAllJournals(){
		final String[] journalPaths = journalPath.toFile().list(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(FileSystemJournal.extention);
            }
        });
		
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
		return this.journalWriter.isClosed();
	}

	@Override
	public void close() throws IOException {
		this.journalWriter.close();
	}
	
	public void write(final byte[] bytes) throws IOException{
		this.write(bytes, 0, bytes.length);
	}
	public void write(final byte[] bytes, int offset, int length) throws IOException{
		ByteBuffer b = bufferPool.acquire(headerByteSize+length, false);
		b.clear();
		b.putInt(length);
		b.put(bytes, offset, length);
		b.flip();
		this.journalWriter.write(b);
	}
	public ByteBuffer write(final ByteBuffer buffer) throws IOException{
		ByteBuffer b = bufferPool.acquire(headerByteSize+buffer.remaining(), false);
		b.clear();
		b.putInt(buffer.remaining());
		b.put(buffer);
		b.flip();
		this.journalWriter.write(b);
		
		return buffer;
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

}
