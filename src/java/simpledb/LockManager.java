package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static simpledb.Permissions.READ_WRITE;
import static simpledb.Permissions.READ_ONLY;

public class LockManager {
    private final Map<PageId, LockSet> locks;

    public LockManager() {
        this.locks = new ConcurrentHashMap<>();
    }

    public Map<PageId, LockSet> getLocks() {
        return locks;
    }

    public void clearTransaction(TransactionId tid) {
        getLocks().forEach((k, v) -> v.release(tid));
    }

    /**
     * A wrapper for HashSet that can be acquired and release in a shared or exclusive fashion
     */
    public static class LockSet {
        boolean shared;
        Set<TransactionId> holders;

        public LockSet() {
            shared = true;
            this.holders = new HashSet<>();
        }

        public synchronized boolean acquire(TransactionId tid, Permissions perm) {
            if (hasLock(tid)) {
                if (perm == READ_WRITE) {
                   if (shared && holders.size() == 1) {
                       shared = false;
                   } else {
                       return !shared;
                   }
                }
            } else if (perm == READ_ONLY && shared) {
                holders.add(tid);
            } else if (perm == READ_ONLY && notHeld()) {
                holders.add(tid);
                shared = true;
            } else if (perm == READ_WRITE && notHeld()) {
                holders.add(tid);
                shared = false;
            } else {
                return false;
            }
            return true;
        }

        public synchronized boolean release(TransactionId tid) {
            return holders.remove(tid);
        }

        public boolean hasLock(TransactionId tid) {
           return holders.contains(tid);
        }

        public boolean notHeld() {
            return holders.isEmpty();
        }
    }
}
