package simpledb;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private Map<Field, Integer> countPerGroup;
    private boolean itOpen;
    private Iterator<Tuple> tuples;
    private TupleDesc tp;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        countPerGroup = new ConcurrentHashMap<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        countPerGroup.merge((gbfield == NO_GROUPING) ? null : tup.getField(gbfield), 1, Integer::sum);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
                tuples = countPerGroup.entrySet().stream().map(entry -> {
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


