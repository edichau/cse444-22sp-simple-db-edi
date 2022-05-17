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

    public Map<PageId, LockSet> getLocks() {
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

    public synchronized boolean wouldDeadlock(TransactionId tid, PageId pid, Permissions permissions) {
        // Adapted from: https://www.geeksforgeeks.org/detect-cycle-in-a-graph/
        Map<TransactionId, Boolean> visited = new ConcurrentHashMap<>();
        Map<TransactionId, Boolean> recursion = new ConcurrentHashMap<>();

        LockSet lockSet = locks.getOrDefault(pid, new LockSet());
        if (!lockSet.acquire(tid, permissions)) {
            return false;
        }

        Set<TransactionId> iterate = ConcurrentHashMap.newKeySet();
        iterate.addAll(lockSet.holders);
        iterate.remove(tid); // Prevents self-cycle detection

        for (TransactionId transactionId : iterate) {
            if (deadlockHelper(transactionId, pid, visited, recursion)) {
                locks.get(pid).release(tid);
                return true;
            }
        }

        return false;
    }

    private synchronized boolean deadlockHelper(TransactionId tid, PageId pid, Map<TransactionId, Boolean> vis, Map<TransactionId, Boolean> rec) {
        if (rec.getOrDefault(tid, false)) {
            return true;
        }

        if (vis.getOrDefault(tid, false)) {
            return false;
        }

        vis.put(tid, true);
        rec.put(tid, true);

        for (PageId p : getTransactionPages(tid)) {
            for (TransactionId transactionId : locks.getOrDefault(p, new LockSet()).holders) {
                if (deadlockHelper(transactionId, pid, vis, rec)) {
                    return true;
                }
            }
        }

        rec.put(tid, false);

        return false;
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
