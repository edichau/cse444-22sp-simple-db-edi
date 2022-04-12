package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private final List<TDItem> items = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // Map type & name to a TDItem and store it in items
        IntStream.range(0, typeAr.length)
                .mapToObj(i -> new TDItem(typeAr[i], (i < fieldAr.length) ? fieldAr[i] : ""))
                .forEachOrdered(items::add);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // Map each type to a TDItem without a name
        Arrays.stream(typeAr)
                .map(t -> new TDItem(t, ""))
                .forEachOrdered(items::add);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
       if (i >= items.size()) {
           throw new NoSuchElementException();
       }
       return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= items.size()) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // Find the first index that matches the field name or throw exception
        return IntStream.range(0, items.size())
                .filter(i -> items.get(i).fieldName.equals(name))
                .findFirst()
                .orElseThrow(NoSuchElementException::new);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return items.stream().mapToInt(td -> td.fieldType.getLen()).sum();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // Generate IntStreams from 0 ... numFields
        Function<TupleDesc, IntStream> str = v -> IntStream.range(0, v.numFields());

        // Stream over both td1 and td2 to concatenate their names and types
        Type[] types = Stream.concat(
                str.apply(td1).mapToObj(td1::getFieldType),
                str.apply(td2).mapToObj(td2::getFieldType)
        ).toArray(Type[]::new);

        String[] names = Stream.concat(
                str.apply(td1).mapToObj(td1::getFieldName),
                str.apply(td2).mapToObj(td2::getFieldName)
        ).toArray(String[]::new);

        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            if (((TupleDesc) o).numFields() == numFields()) {
                for (int i = 0; i < numFields(); i++) {
                    if (!((TupleDesc) o).getFieldType(i).equals(getFieldType(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        int prime = 37;
        int fds = numFields();
        int result = 1;

        result = prime * result + fds;

        for (TDItem td : items) {
           result = prime * result + td.fieldType.hashCode();
        }

        return result;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // Convert items to strings and join them with ", "
        return items.stream().map(TDItem::toString).collect(Collectors.joining(", "));
    }
}
