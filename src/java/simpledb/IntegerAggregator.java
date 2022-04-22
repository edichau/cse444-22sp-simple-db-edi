package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final Map<Field, Integer> aggregate;
    private Map<Field, Integer> countPerGroup;
    private boolean itOpen;
    private Iterator<Tuple> tuples;
    private TupleDesc tp;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        aggregate = new ConcurrentHashMap<>();
        countPerGroup = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int toMerge = ((IntField) tup.getField(afield)).getValue();
        Field key = (gbfield != NO_GROUPING) ? tup.getField(gbfield) : null;
        countPerGroup.merge(key, 1, Integer::sum);
        BiFunction<Integer, Integer, Integer> method;
        switch (what) {
            case MIN: method = Math::min; break;
            case MAX: method = Math::max; break;
            case SUM: method = Math::addExact; break;
            case AVG:
                int n = countPerGroup.get(key);
                method = (integer, integer2) -> (integer * (n - 1) + integer2) / n;
                break;
            case COUNT:
                aggregate.putIfAbsent(key, 0);
                method = (integer, integer2) -> integer + 1;
                break;
            default: method = null;
        }
        assert method != null;
        aggregate.merge(key, toMerge, method);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            @Override
            public void open() throws DbException, TransactionAbortedException {
                itOpen = true;
                if (gbfield != NO_GROUPING) {
                    tp = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateVal"});
                } else {
                    tp = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
                }
                tuples = aggregate.entrySet().stream().map(entry -> {
                    Tuple tuple = new Tuple(tp);
                    if (gbfield != NO_GROUPING) {
                        tuple.setField(0, entry.getKey());
                        tuple.setField(1, new IntField(entry.getValue()));
                    } else {
                        tuple.setField(0, new IntField(entry.getValue()));
                    }
                    return tuple;
                }).iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return itOpen && tuples.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return tuples.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                boolean state = itOpen;
                open();
                itOpen = state;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tp;
            }

            @Override
            public void close() {
                itOpen = false;
            }
        };
    }

}
