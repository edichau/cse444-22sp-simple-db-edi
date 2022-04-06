package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final int id;

    // Fields for iterator to keep track of tuple
    private Iterator<Tuple> itr;
    private int pid;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // Page does not exist
        if (pid.getPageNumber() > numPages()) {
            throw new IllegalArgumentException();
        }

        Page page = null;
        try {
            // Setup file for reading and data storage
            RandomAccessFile read = new RandomAccessFile(getFile(), "r");
            int pageSize = BufferPool.getPageSize();
            byte[] data = new byte[pageSize];

            // Read from the offset we seek in the file
            read.seek((long) pageSize * pid.getPageNumber());
            read.read(data, 0, pageSize);

            // Convert data back to HeapPage
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (page == null) {
            throw new IllegalArgumentException();
        }

        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            @Override
            public void open() throws DbException, TransactionAbortedException {
                pid = 0;
                itr = getItr(pid);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (itr == null) return false;
                // Walk through the page iterators searching for the next Tuple
                while (pid < numPages() - 1 && !itr.hasNext()) {
                    pid++;
                    itr = getItr(pid);
                }
                return itr != null && itr.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return itr.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // Re-open iterator, effectively resetting it
                open();
            }

            @Override
            public void close() {
                itr = null;
            }

            private Iterator<Tuple> getItr(int pageNum) throws DbException, TransactionAbortedException{
                return ((HeapPage) Database.getBufferPool()
                        .getPage(tid, new HeapPageId(getId(), pageNum), Permissions.READ_WRITE))
                        .iterator();
            }
        };
    }

}

