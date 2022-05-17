package simpledb;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static simpledb.Permissions.READ_WRITE;
import static simpledb.Permissions.READ_ONLY;

public class LockManager {
    private final Map<PageId, LockSet> locks;

    public LockManager() {
        this.locks = new ConcurrentHashMap<>();
    }

    public synchronized Map<PageId, LockSet> getLocks() {
        return locks;
    }

    public synchronized void clearTransaction(TransactionId tid) {
        getLocks().forEach((k, v) -> v.release(tid));
    }

    public synchronized Set<PageId> getTransactionPages(TransactionId tid) {
        return getLocks().entrySet().stream()
                .filter(e -> e.getValue().hasLock(tid))
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));
    }

    /**
     * A wrapper for HashSet that can be acquired and release in a shared or exclusive fashion
     */
    public static class LockSet {
        boolean shared;
        Set<TransactionId> holders;

        public LockSet() {
            shared = true;
            holders = ConcurrentHashMap.newKeySet();
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

        public synchronized boolean hasLock(TransactionId tid) {
           return holders.contains(tid);
        }

        public synchronized boolean notHeld() {
            return holders.isEmpty();
        }
    }
}
