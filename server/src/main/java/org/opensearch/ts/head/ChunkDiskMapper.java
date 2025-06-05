/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ts.head;

import org.opensearch.ts.chunks.Chunk;
import org.opensearch.ts.chunks.Encoding;
import org.opensearch.ts.chunks.ImmutableRawChunk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

/**
 * Write and mmap head chunks to disk. Chunks are appended sequentially to files in the specified directory. Files are created as needed
 * when the current file reaches the maximum size, or if a new ChunkDiskMapper instance is created. Writing chunks is sequential, with
 * buffering deferred to the os. A successful write returns a {@link ChunkRef} that can be used to read the chunk later. Read operations
 * return a slice the mmapped buffer.
 */
public class ChunkDiskMapper {
    public static final String CHUNK_FILE_PREFIX = "chunk_";
    public static final int CHUNK_FILE_HEADER_SIZE = Integer.BYTES * 2; // magic bytes, version
    public static final int CHUNK_FILE_MAGIC_BYTES = Integer.decode("0x0123BC01");
    public static final int CHUNK_FILE_VERSION = 1;

    // seriesRef, minTimestamp, maxTimestamp, encoding, chunk bytes len, crc32
    public static final int CHUNK_META_SIZE = Long.BYTES * 4 + Integer.BYTES * 2;

    // 512MB default max file size for chunk files
    private static final int DEFAULT_MAX_CHUNK_FILE_SIZE = 512 * 1024 * 1024;

    private int nextFileIndex;
    private final int maxChunkFileSize;
    private final Path dir;
    private final LinkedHashMap<Integer, ChunkFile> chunkFiles = new LinkedHashMap<>(); // file index to chunk file index
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final CRC32 crc32 = new CRC32();

    public ChunkDiskMapper(Path dir) {
        this(dir, DEFAULT_MAX_CHUNK_FILE_SIZE); // default max chunk file size is 512MB
    }

    public ChunkDiskMapper(Path dir, int maxChunkFileSize) {
        this.dir = dir;
        this.maxChunkFileSize = maxChunkFileSize;

        loadExistingFiles();
    }

    public int getNumFiles() {
        lock.readLock().lock();
        try {
            return chunkFiles.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return ChunkRef used for reading the chunk later.
     */
    public ChunkRef writeChunk(long seriesRef, MemChunk chunk) {
        long minTimestamp = chunk.getMinTimestamp();
        long maxTimestamp = chunk.getMaxTimestamp();
        Encoding encoding = chunk.getChunk().encoding();

        byte[] chunkBytes = chunk.getChunk().bytes();
        int totalSize = CHUNK_META_SIZE + chunkBytes.length;

        lock.writeLock().lock();
        try {
            ensureCapacity(totalSize);
            assert !chunkFiles.isEmpty() : "ensureCapacity guarantees the last chunk file has space for writing";

            // TODO pool and move outside lock?
            crc32.reset();
            crc32.update(chunkBytes, 0, chunkBytes.length);

            ByteBuffer buffer = chunkFiles.lastEntry().getValue().getRwMappedBuffer();
            buffer.putLong(seriesRef);
            buffer.putLong(minTimestamp);
            buffer.putLong(maxTimestamp);
            buffer.putInt(encoding.ordinal()); // TODO: ooo mask
            buffer.putInt(chunkBytes.length);
            buffer.put(chunk.getChunk().bytes());
            buffer.putLong(crc32.getValue());

            return new ChunkRef(chunkFiles.size() - 1, buffer.position() - totalSize, totalSize);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Ensures that there is enough space in the file to write the next chunk. If not, open a new file for writing.
     */
    private void ensureCapacity(int requiredSize) {
        if (!chunkFiles.isEmpty()
            && (chunkFiles.lastEntry().getValue().getRwMappedBuffer().position() + requiredSize) <= maxChunkFileSize) {
            return;
        }
        chunkFiles.put(nextFileIndex, new ChunkFile(dir, maxChunkFileSize, nextFileIndex, false));
        nextFileIndex++;
    }

    /**
     * Returns a slice corresponding to one chunk that can be used for reading.
     */
    public Chunk chunkFor(ChunkRef chunkRef) {
        ByteBuffer buffer = chunkFiles.get(chunkRef.fileIndex()).roSlice(chunkRef.offset, chunkRef.size);

        // Skip seriesRef, minTimestamp, maxTimestamp
        buffer.position((3 * Long.BYTES));
        Encoding encoding = Encoding.values()[buffer.getInt()];
        int chunkBytesLen = buffer.getInt();

        // Copy the chunk bytes into a new byte array, in case buffer is closed while reading
        // TODO: we may also pass the slice to avoid maintaining on heap copy, but may require more complex buffer lifecycle management
        byte[] chunkBytes = new byte[chunkBytesLen];
        buffer.get(chunkBytes);

        // skip crc check here, it shall be validated when the file is loaded
        return switch (encoding) {
            case RAW -> new ImmutableRawChunk(chunkBytes);
            case XOR -> throw new UnsupportedOperationException("Not implemented yet");
        };
    }

    /**
     * Returns an iterator over all chunks in the disk mapper. The chunks are read from the mmapped files and CRCs are verified.
     * This is used during server initialization and is NOT thread safe, writeChunk should not be called until the iterator is
     * no longer needed.
     */
    public Iterator<ChunkAndSeriesRef> chunkIterator() {
        return new Iterator<>() {
            private final Iterator<Map.Entry<Integer, ChunkFile>> fileIterator = chunkFiles.sequencedEntrySet().iterator();
            private ByteBuffer currentBuffer;
            private int currentFileIndex;
            private ChunkAndSeriesRef nextChunk;

            @Override
            public boolean hasNext() {
                if (nextChunk != null) {
                    return true;
                }
                nextChunk = loadNextChunk();
                return nextChunk != null;
            }

            @Override
            public ChunkAndSeriesRef next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                ChunkAndSeriesRef result = nextChunk;
                nextChunk = null;
                return result;
            }

            /**
             * Loads the next chunk from the current buffer or advances to the next file if needed.
             * Returns null if no more chunks are available.
             */
            private ChunkAndSeriesRef loadNextChunk() {
                while (true) {
                    if (currentBuffer == null || !currentBuffer.hasRemaining()) {
                        if (!advanceToNextFile()) {
                            return null; // no more files
                        }
                    }

                    if (currentBuffer.remaining() < CHUNK_META_SIZE) {
                        // Not enough data for a chunk header, move to next file
                        currentBuffer = null;
                        continue;
                    }

                    if (isEndOfDataMarker(currentBuffer)) {
                        // Reached end marker (all zeroes), move to next file
                        currentBuffer = null;
                        continue;
                    }

                    int chunkStartPosition = currentBuffer.position();

                    // Read chunk header fields
                    long seriesRef = currentBuffer.getLong();
                    long minTimestamp = currentBuffer.getLong();
                    long maxTimestamp = currentBuffer.getLong();
                    int encodingOrdinal = currentBuffer.getInt();
                    if (encodingOrdinal < 0 || encodingOrdinal >= Encoding.values().length) {
                        throw new RuntimeException("Invalid encoding ordinal: " + encodingOrdinal); // todo corruption exception
                    }
                    Encoding encoding = Encoding.values()[encodingOrdinal];
                    int chunkBytesLen = currentBuffer.getInt();

                    if (currentBuffer.remaining() < chunkBytesLen + Long.BYTES) {
                        // Not enough data for chunk bytes and CRC, move to next file
                        currentBuffer = null;
                        continue;
                    }

                    byte[] chunkBytes = new byte[chunkBytesLen];
                    currentBuffer.get(chunkBytes);
                    long expectedCrc = currentBuffer.getLong();

                    crc32.reset();
                    crc32.update(chunkBytes);
                    long actualCrc = crc32.getValue();
                    if (actualCrc != expectedCrc) {
                        throw new RuntimeException("CRC mismatch for chunk in file index " + currentFileIndex); // todo corruption exception
                    }

                    MMappedChunk mappedChunk =
                        new MMappedChunk(new ChunkRef(currentFileIndex, chunkStartPosition, CHUNK_META_SIZE + chunkBytesLen),
                            minTimestamp,
                            maxTimestamp
                        );

                    return new ChunkAndSeriesRef(mappedChunk, seriesRef);
                }
            }

            /**
             * Advances to the next file and initializes the currentBuffer.
             * Returns true if a valid file buffer is loaded, false if no more files.
             */
            private boolean advanceToNextFile() {
                while (fileIterator.hasNext()) {
                    Map.Entry<Integer, ChunkFile> entry = fileIterator.next();
                    currentFileIndex = entry.getKey();
                    ChunkFile chunkFile = entry.getValue();
                    currentBuffer = chunkFile.getRoMappedBuffer();

                    if (currentBuffer == null || currentBuffer.remaining() < CHUNK_FILE_HEADER_SIZE) {
                        // Empty or invalid file, skip it
                        currentBuffer = null;
                        continue;
                    }

                    int magicBytes = currentBuffer.getInt();
                    int version = currentBuffer.getInt();
                    if (magicBytes != CHUNK_FILE_MAGIC_BYTES || version != CHUNK_FILE_VERSION) {
                        throw new RuntimeException(
                            "Corrupt head chunk file: " + chunkFile.file.getAbsolutePath()); // todo corruption exception
                    }

                    return true;
                }
                return false; // no more files
            }

            /**
             * Checks if the next chunk header is all zeroes, indicating end of data in the file.
             */
            private boolean isEndOfDataMarker(ByteBuffer buffer) {
                int pos = buffer.position();
                // todo check the entire file, in case of non sequential write/corruption?
                for (int i = 0; i < CHUNK_META_SIZE; i++) {
                    if (buffer.get(pos + i) != 0) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    /**
     * @param fileIndex remove all chunk files with file index strictly less than this value.
     */
    public void truncate(long fileIndex) {
        lock.writeLock().lock();
        try {
            int i = 0;
            while (i < chunkFiles.size()) {
                ChunkFile chunkFile = chunkFiles.get(i);
                if (chunkFile.getFileIndex() >= fileIndex) {
                    return;
                }
                chunkFile.destroy();
                chunkFiles.remove(chunkFile.getFileIndex());
                i++;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close when no more reads or writes will take place.
     */
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            for (ChunkFile chunkFile : chunkFiles.sequencedValues()) {
                chunkFile.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * mmap any existing files in the directory (e.g., after restart)
     */
    private void loadExistingFiles() {
        File[] files = dir.toFile().listFiles();
        if (files == null) {
            return;
        }

        // Sort by name to process in order of the file index
        Arrays.sort(files, Comparator.comparing(File::getName));

        for (File file : files) {
            if (!(file.isFile() && file.getName().startsWith(CHUNK_FILE_PREFIX))) {
                continue; // skip non-chunk files
            }
            try {
                int fileIndex = Integer.parseInt(file.getName().substring(CHUNK_FILE_PREFIX.length()));
                ChunkFile chunkFile = new ChunkFile(dir, (int) file.length(), fileIndex, true);
                chunkFiles.put(fileIndex, chunkFile);
                nextFileIndex = Math.max(nextFileIndex, fileIndex + 1);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid chunk file name: " + file.getName(), e);
            }
        }
        // last chunk file has data, create a new file for future writes
        chunkFiles.put(nextFileIndex, new ChunkFile(dir, maxChunkFileSize, nextFileIndex, false));
        nextFileIndex++;
    }

    /**
     * Reference to a mapped head chunk.
     */
    public record ChunkRef(int fileIndex, int offset, int size) {}

    /**
     * Contains a mapped chunk and its metadata, used when loading existing head chunks from disk.
     */
    public record ChunkAndSeriesRef(MMappedChunk chunk, long seriesRef) {}

    /**
     * Represents a chunk file that is mapped to memory for reading and writing.
     */
    private static class ChunkFile {
        private final int fileIndex;
        private final File file;
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final ByteBuffer writeBuffer;
        private final ByteBuffer readBuffer;

        ChunkFile(Path dir, int maxFileSize, int fileIndex, boolean exists) {
            try {
                this.fileIndex = fileIndex;
                this.file = new File(dir.toFile(), CHUNK_FILE_PREFIX + fileIndex);
                this.raf = new RandomAccessFile(file, "rw");
                this.channel = raf.getChannel();

                // TODO: use ffm api? requires --enable-preview
                this.writeBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileSize).order(ByteOrder.BIG_ENDIAN);
                this.readBuffer = writeBuffer.asReadOnlyBuffer();

                if (!exists) {
                    // write header
                    writeBuffer.putInt(CHUNK_FILE_MAGIC_BYTES);
                    writeBuffer.putInt(CHUNK_FILE_VERSION);
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Could not open file for writing: " + dir, e);
            } catch (IOException e) {
                throw new RuntimeException("Could not map file to memory", e);
            }
        }

        int getFileIndex() {
            return fileIndex;
        }

        ByteBuffer getRwMappedBuffer() {
            return writeBuffer;
        }

        ByteBuffer getRoMappedBuffer() {
            return readBuffer.duplicate();
        }

        /**
         * @return read-only slice of the file
         */
        ByteBuffer roSlice(int offset, int length) {
            return readBuffer.slice(offset, length);
        }

        /**
         * Close the chunk file channel, truncating it to the current write position and fsync.
         */
        void close() {
            try {
                int fileLength = writeBuffer.position();
                channel.truncate(fileLength);
                channel.force(true);
                channel.close();
                raf.close();
            } catch (IOException e) {
                throw new RuntimeException("Could not close chunk file channel", e);
            }
        }

        /**
         * Destroy the chunk file by truncating it to zero length, closing the channel, and deleting the file.
         */
        void destroy() {
            try {
                channel.truncate(0);
                channel.force(true);
                channel.close();
                raf.close();
                file.delete();
            } catch (IOException e) {
                // TODO on windows we'd likely see an exception while deleting as file has not been unmapped, catch silently instead?
                //   --enable-preview for ffm api?
                throw new RuntimeException("Could not destroy chunk file: " + file.getAbsolutePath(), e);
            }
        }
    }
}
