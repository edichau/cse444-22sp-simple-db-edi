package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    // The internal representation of the buffer pool
    private final Map<PageId, Page> buffer;

    // The maximum number of pages
    private final int maxCapacity;

    // Manager for the page locks by transactions
    private final LockManager lockManager;

    // Dependency graph for cycle detection
    private final Map<WaitFor, Set<WaitFor>> deps;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        buffer = new ConcurrentHashMap<>();
        maxCapacity = numPages;
        lockManager = new LockManager();
        deps = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        WaitFor waitFor = new WaitFor(tid, pid);
        LockManager.LockSet lockSet = getHolders().getOrDefault(pid, new LockManager.LockSet());
        Set<WaitFor> holders = deps.getOrDefault(waitFor, ConcurrentHashMap.newKeySet());

        lockSet.holders.stream().filter(t -> !t.equals(tid)).map(t -> new WaitFor(t, pid)).forEach(holders::add);
        deps.put(waitFor, holders);

        for (Map.Entry<WaitFor, Set<WaitFor>> waiting : deps.entrySet()) {
           if (waiting.getKey().pid.equals(pid) && !waiting.getKey().tid.equals(tid)) {
               Set<WaitFor> add = waiting.getValue();
               add.add(waitFor);
               deps.put(waiting.getKey(), add);
           }
        }

        // Waits until it can acquire the page before proceeding
        getHolders().putIfAbsent(pid, new LockManager.LockSet());
        while (!getHolders().get(pid).acquire(tid, perm)) {
            try {
                // Cycle detection similar to https://www.geeksforgeeks.org/detect-cycle-in-a-graph/
                if (cyclic()) {
                    throw new TransactionAbortedException();
                }

                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        deps.remove(waitFor);
        deps.forEach((k, v) -> v.remove(waitFor));

        // If buffer does not have page get it using the HeapFile
        if (!buffer.containsKey(pid)) {
            if (buffer.size() == maxCapacity) {
                evictPage();
            }
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            buffer.put(pid, page);
        }
        return buffer.get(pid);
    }

    private synchronized boolean cyclic() {
        Map<TransactionId, Boolean> visited = new ConcurrentHashMap<>();
        Map<TransactionId, Boolean> recursion = new ConcurrentHashMap<>();

        for (WaitFor waiting : deps.keySet()) {
            if (dfs(waiting, visited, recursion)) {
                return true;
            }
        }

        return false;
    }

    private synchronized boolean dfs(WaitFor waiting, Map<TransactionId, Boolean> vis, Map<TransactionId, Boolean> rec) {
        TransactionId tid = waiting.tid;

        if (rec.getOrDefault(tid, false)) {
            return true;
        }

        if (vis.getOrDefault(tid, false)) {
            return false;
        }

        vis.put(tid, true);
        rec.put(tid, true);

        for (WaitFor w : deps.getOrDefault(waiting, ConcurrentHashMap.newKeySet())) {
            if (dfs(w, vis, rec)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        WaitFor waitFor = new WaitFor(tid, pid);

        // Remove this tid/page combo
        deps.remove(waitFor);

        // Other transactions are no-longer depending on its completion
        deps.forEach((k, v) -> v.remove(waitFor));

        // Release all the locks
        getHolders().getOrDefault(pid, new LockManager.LockSet()).release(tid);
        notifyAll();
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    private synchronized Map<PageId, LockManager.LockSet> getHolders() {
        return lockManager.getLocks();
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        return getHolders().containsKey(p) && getHolders().get(p).hasLock(tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if (commit){
            flushPages(tid);
        } else {
            for (PageId pid : lockManager.getTransactionPages(tid)) {
                HeapPage page = (HeapPage) Database.getCatalog()
                        .getDatabaseFile(pid.getTableId())
                        .readPage(pid);
                page.markDirty(false, tid);
                buffer.put(pid, page);
            }
        }

        // Remove all the dependencies with this tid
        deps.forEach((k, v) -> {
            if (k.tid.equals(tid)) {
                deps.remove(k);
            }
        });

        // Remove this transaction and all its page combos from every dependency
        for (PageId page : lockManager.getTransactionPages(tid)) {
            deps.forEach((k, v) -> v.remove(new WaitFor(tid, page)));
        }

        // Cleanup the locks
        lockManager.clearTransaction(tid);
        notifyAll();
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            buffer.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public synchronized void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            buffer.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId : buffer.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        HeapPage page = (HeapPage) buffer.get(pid);
        if (page != null && page.dirty) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (PageId pageId : lockManager.getTransactionPages(tid)) {
            flushPage(pageId);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // Exception if there are no clean pages in the buffer pool
        Supplier<DbException> exception = () -> new DbException("No clean pages to evict");

        // Get the first clean page in the buffer pool
        PageId pageId = buffer.entrySet().stream()
                .filter(e -> !((HeapPage) e.getValue()).dirty)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(exception);

        // Attempt to remove the found page from the buffer pool
        try {
            flushPage(pageId);
            buffer.remove(pageId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class WaitFor {
        TransactionId tid;
        PageId pid;

        public WaitFor(TransactionId tid, PageId pid) {
            this.tid = tid;
            this.pid = pid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof WaitFor)) return false;

            WaitFor waitFor = (WaitFor) o;

            if (!tid.equals(waitFor.tid)) return false;
            return pid.equals(waitFor.pid);
        }

        @Override
        public String toString() {
            return String.format("WaitFor{tid=%s, pid=%s}", tid.myid, pid.getPageNumber());
        }

        @Override
        public int hashCode() {
            int result = tid.hashCode();
            result = 31 * result + pid.hashCode();
            return result;
        }
    }

}
