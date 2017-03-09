package com.buabook.kdb.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.buabook.kdb.Converters;
import com.buabook.kdb.exceptions.DataOverwriteNotPermittedException;
import com.buabook.kdb.exceptions.TableColumnAlreadyExistsException;
import com.buabook.kdb.exceptions.TableSchemaMismatchException;
import com.buabook.kdb.query.KdbQuery;
import com.google.common.base.Strings;
import com.kx.c;
import com.kx.c.Dict;
import com.kx.c.Flip;

/**
 * <h2>Table Data Container for kdb</h2>
 * <p>Data container representing a table that can be serialised and sent to a kdb process (via {@link c})</p>
 * <p>Also provides the ability to accept a {@link Flip} object received from a kdb process and access it 
 * within Java, including iterating over it with the {@link KdbDict} object.</p>
 * <p>Implementation is <i>not</i> thread-safe.</p>
 * (c) 2014 - 2017 Sport Trades Ltd
 * 
 * @see KdbTableIterator
 * @see KdbDict
 *
 * @author Jas Rajasansir
 * @version 1.1.0
 * @since 1 Apr 2014
 */
public class KdbTable implements Iterable<KdbDict> {
	private static final Logger log = LoggerFactory.getLogger(KdbTable.class);
	
	
	/** The table name for the current instance */
	private String tableName;
	
	/** The container of the kdb data stored column-wise */
	private final Map<String, List<Object>> data;
	
	private Integer rowCount;

	
	/**
	 * Instantiate an empty table structure
	 * @param tableName The name of the table to create
	 * @throws IllegalArgumentException If the table name is empty or null
	 */
	public KdbTable(String tableName) throws IllegalArgumentException {
		if(Strings.isNullOrEmpty(tableName))
			throw new IllegalArgumentException();
		
		this.tableName = tableName;
		this.data = new HashMap<>();
		this.rowCount = 0;
	}
	
	/**
	 * Instantiates a new kdb table object with a set of initial data in kdb format
	 * @param tableName The name of the table to create
	 * @param initialData The initial data set to populate the new object with
	 * @throws IllegalArgumentException If the table name is empty or null
	 * @see #setInitialDataSet(Flip)
	 */
	public KdbTable(String tableName, Flip initialData) throws DataOverwriteNotPermittedException {
		this(tableName);

		setInitialDataSet(initialData);
	}
	
	/**
	 * Performs the initial set of data from a kdb {@link Flip} object
	 * @param initialData The dataset to use to set the object
	 * @throws DataOverwriteNotPermittedException If there is already data in the current data structure
	 * @see #doSetOfInitialDataSet(Flip, boolean)
	 */
	public void setInitialDataSet(Flip initialData) throws DataOverwriteNotPermittedException {
		doSetOfInitialDataSet(initialData, false);
	}
	
	/**
	 * Performs the initial (or subsequent) set of data from a kdb {@link Flip} object
	 * @param initialData The dataset to use to set the object
	 * @see #doSetOfInitialDataSet(Flip, boolean)
	 */
	public void forceSetInitialDataSet(Flip initialData) {
		doSetOfInitialDataSet(initialData, true);
	}
	
	private void doSetOfInitialDataSet(Flip initialData, boolean overwrite) throws DataOverwriteNotPermittedException {
		if(! isEmpty()) {
			if(! overwrite) {
				log.error("Data already exists and overwrite not set [ Table: {} ]", tableName);
				throw new DataOverwriteNotPermittedException();
			}
		}
		
		data.clear();
		
		for(int cCount = 0; cCount < initialData.x.length; cCount++) {
			String colName = initialData.x[cCount];
			Object[] colData = Converters.arrayToObjectArray(initialData.y[cCount]);
			List<Object> objArray = new ArrayList<>(Arrays.asList(colData));
			
			data.put(colName, objArray);
		}
		
		this.rowCount = Converters.arrayToObjectArray(initialData.y[0]).length;
	}
	
	/**
	 * Adds a new column to the current table stored within the object. 
	 * @param columnName The name of the new column
	 * @param columnData The list of new elements of the column
	 * @throws TableColumnAlreadyExistsException If there is a column with the same name as the one to be added
	 * @throws TableSchemaMismatchException If the length of the new column does not match the current number of rows in the table
	 */
	public void addColumn(String columnName, List<Object> columnData) throws TableColumnAlreadyExistsException, TableSchemaMismatchException {
		if(data.containsKey(columnName))
			throw new TableColumnAlreadyExistsException("[ Column: " + columnName + " ]");
		
		if(columnData.size() != rowCount)
			throw new TableSchemaMismatchException("Column length (" + columnData.size() + ") does not match the current number of rows (" + rowCount + ")!");
		
		data.put(columnName, columnData);
	}
	
	public void deleteColumn(String columnName) {
		if(Strings.isNullOrEmpty(columnName))
			throw new IllegalArgumentException("No column name specified to delete");
		
		data.remove(columnName);
	}
	
	/** @see #addRow(Map) */
	public void addRow(KdbDict row) throws TableSchemaMismatchException {
		if(row == null)
			return;
		
		addRow(row.getDataStoreWithStringKeys());
	}
	
	/**
	 * Adds a row to a table based on an {@link HashMap} representation of the row (i.e. a kdb dictionary)
	 * @param row The new row to add
	 * @throws TableSchemaMismatchException If there are any missing columns from the new row
	 */
	public void addRow(Map<String, Object> row) throws TableSchemaMismatchException {
		if(row == null || row.isEmpty())
			return; 
		
		if(! isEmpty())
			if(! row.keySet().containsAll(data.keySet()))
				throw new TableSchemaMismatchException("Missing columns in row to add");
		
		for(String key : row.keySet()) {
			if(! data.containsKey(key))
				data.put(key, new ArrayList<Object>());
			
			Object rowValue = row.get(key);
			
			if(rowValue instanceof List<?>)
				rowValue = ((List<?>) rowValue).toArray();
			else if(rowValue instanceof Enum)
				rowValue = ((Enum<?>) rowValue).toString();
			
			data.get(key).add(rowValue);
		}
		
		this.rowCount++;
	}
	
	/**
	 * Appends the specified table onto the current table (similar to the kdb+ <code>uj</code> function) 
	 * @throws TableSchemaMismatchException If the two table names or table schemas do not match
	 */
	public void append(KdbTable that) throws TableSchemaMismatchException {
		if(that == null || that.isEmpty())
			return;
		
		if(! this.tableName.equals(that.tableName))
			throw new TableSchemaMismatchException("Table names are different");
		
		if(! isEmpty())
			if(! that.getTableData().keySet().containsAll(data.keySet()))
				throw new TableSchemaMismatchException("Tables have different schemas");
		
		for(Entry<String, List<Object>> column : that.getTableData().entrySet()) {
			if(! data.containsKey(column.getKey()))
				data.put(column.getKey(), new ArrayList<Object>());
			
			data.get(column.getKey()).addAll(column.getValue());
		}
		
		this.rowCount+=that.getRowCount();
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public Map<String, List<Object>> getTableData() {
		return data;
	}
	
	/**
	 * Converts the nice Java representation of a kdb table into the actual format ready for sending across
	 * the wire to a kdb process. <b>NOTE</b>: The columns of the generated table will <i>always</i> be in 
	 * alphabetical order.
	 * @return The object that can be sent to a kdb process
	 */
	public Flip convertToFlip() {
		if(isEmpty())
			return null;

		// Used TreeSet to enforce alphabetical (and deterministic) ordering of column names 
		TreeSet<String> orderedCols = new TreeSet<>(data.keySet());
		
		String[] colNames = orderedCols.toArray(new String[0]);
		Object[] cols = new Object[data.keySet().size()];
		
		log.trace("Generating kdb Flip object [ Table: {} ] [ Row Count: {} ] [ Columns: {} ]", tableName, rowCount, orderedCols);
		
		for(int kCount = 0; kCount < colNames.length; kCount++)
			cols[kCount] = data.get(colNames[kCount]).toArray();
		
		return new Flip(new Dict(colNames, cols));
	}
	
	public Integer getRowCount() {
		return rowCount;
	}
	
	public Boolean isEmpty() {
		return rowCount == 0 || data.isEmpty();
	}
	
	/**
	 * Allows the name of the table stored within this object to be changed.
	 * @param newTableName The new table name
	 * @throws UnsupportedOperationException If the new table name is <code>null</code> or an empty string
	 */
	public void changeTableName(String newTableName) throws UnsupportedOperationException {
		if(Strings.isNullOrEmpty(newTableName))
			throw new UnsupportedOperationException("Table name cannot be null or empty");
		
		this.tableName = newTableName;
	}
	
	/**
	 * Provides the ability to return a specified row of the table. The row is provided as a {@link KdbDict}.
	 * @param rowNumber The row to retrieve
	 * @return The row
	 * @throws ArrayIndexOutOfBoundsException If the row requested is less than 0 or greater than or 
	 * equal to the number of rows in the table
	 */
	public KdbDict getRow(int rowNumber) throws ArrayIndexOutOfBoundsException {
		if(rowNumber < 0 || rowNumber >= rowCount)
			throw new ArrayIndexOutOfBoundsException(rowNumber);
		
		KdbDict row = new KdbDict();
		// Used TreeSet to enforce alphabetical (and deterministic) ordering of column names 
		TreeSet<String> orderedCols = new TreeSet<>(data.keySet());
		
		for(String column : orderedCols) {
			Object cell = data.get(column).get(rowNumber);
			row.add(column, cell, cell.getClass());
		}
		
		return row;
	}
	
	/**
	 * Accepts a list of {@link KdbDict} objects and generates a new {@link KdbTable} from it
	 * @param name The name of the new table
	 * @param rows The list of rows to generate the table from
	 * @return The new table
	 * @see #addRow(KdbDict)
	 */
	public static KdbTable buildFromRowList(String name, List<KdbDict> rows) throws TableSchemaMismatchException {
		KdbTable table = new KdbTable(name);
		
		rows.forEach(table::addRow);
		
		return table;
	}
	
	/**
	 * <p>Generates a new {@link KdbTable} object from an object. This method will
	 * only succeed if the object provided can be cast into a {@link Flip}.</p>
	 * <p>Use this method along with {@link KdbQuery} to convert a query result
	 * into a table when one is expected.</p> 
	 * @param object The object to generate a new {@link KdbTable} from
	 * @return A new table or <code>null</code> if the provided object is null
	 * @throws ClassCastException If the specified object cannot be cast into a {@link Flip}
	 */
	public static KdbTable fromObject(Object object) throws ClassCastException {
		if(object == null)
			return null;
		
		return new KdbTable("table", (Flip) object);
	}

	@Override
	public Iterator<KdbDict> iterator() {
		return new KdbTableIterator(this);
	}
	
	/** TODO: Implement {@link Collection} interface so this is an override */
	public Stream<KdbDict> stream() {
		Iterable<KdbDict> iterable = () -> iterator();
		return StreamSupport.stream(iterable.spliterator(), false);
	}
}
