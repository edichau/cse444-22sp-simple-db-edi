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

    private File file;
    private TupleDesc td;
    private final int id;

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

            // Cleanup
            read.close();
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
        try {
            // Setup file for writing and page data to store
            int pageSize = BufferPool.getPageSize();
            RandomAccessFile write = new RandomAccessFile(getFile(), "rw");
            byte[] pageData = page.getPageData();

            // Write to the offset of the specific page
            write.seek((long) pageSize * page.getId().getPageNumber());
            write.write(pageData);

            // Cleanup
            write.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException();
        }
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
        int pgNo;
        HeapPage page = null;
        for (pgNo = 0; pgNo < numPages(); pgNo++) {
           page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_WRITE);
           if (page.getNumEmptySlots() > 0) {
               break;
           }
        }

        if (pgNo == numPages()) {
            try {
                FileOutputStream fw = new FileOutputStream(getFile(), true);
                fw.write(new HeapPage(new HeapPageId(getId(), numPages()), new byte[BufferPool.getPageSize()]).getPageData());
                fw.close();
                page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_WRITE);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Error reading or writing file");
            }
        }

        assert page != null;
        page.insertTuple(t);

        return new ArrayList<>(List.of(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(List.of(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private int pid;
            private Iterator<Tuple> itr;

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
                        .getPage(tid, new HeapPageId(getId(), pageNum), Permissions.READ_ONLY))
                        .iterator();
            }
        };
    }

}

