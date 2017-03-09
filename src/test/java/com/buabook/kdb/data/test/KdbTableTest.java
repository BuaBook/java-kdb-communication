package com.buabook.kdb.data.test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.buabook.kdb.data.KdbDict;
import com.buabook.kdb.data.KdbTable;
import com.buabook.kdb.exceptions.DataOverwriteNotPermittedException;
import com.buabook.kdb.exceptions.TableColumnAlreadyExistsException;
import com.buabook.kdb.exceptions.TableSchemaMismatchException;
import com.google.common.collect.ImmutableList;
import com.kx.c.Dict;
import com.kx.c.Flip;

public class KdbTableTest {

	// KdbTable(String)

	@Test(expected=IllegalArgumentException.class)
	public void testConstructorThrowsExceptionIfNullTableName() {
		new KdbTable(null);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testConstructorThrowsExceptionIfEmptyTableName() {
		new KdbTable("");
	}

	@Test
	public void testConstructorExecutesOkIfTableNameSpecified() {
		KdbTable table = new KdbTable("my-test-table");
		assertThat(table, is(not(nullValue())));
	}
	
	// KdbTable.setInitialDataSet
	
	@Test
	public void testSetInitialDataSetSetsDataInEmptyTable() {
		KdbTable table = new KdbTable("my-test-table");
		table.setInitialDataSet(getTable());
		
		assertThat(table.getRowCount(), is(equalTo(3)));
		assertThat(table.getTableData(), hasKey("key1"));
		assertThat(table.getTableData(), hasKey("key2"));
		assertThat(table.getTableData(), hasKey("key3"));
	}
	
	@Test(expected=DataOverwriteNotPermittedException.class)
	public void testSetInitialDataSetThrowsExceptionIfTableIsNotEmpty() {
		KdbTable table = new KdbTable("my-test-table");
		table.setInitialDataSet(getTable());
		table.setInitialDataSet(getTable());
	}
	
	// KdbTable.forceSetInitialDataSet
	
	@Test
	public void testForceSetInitialDataSetSetsDataInEmptyTable() {
		KdbTable table = new KdbTable("my-test-table");
		table.forceSetInitialDataSet(getTable());
		
		assertThat(table.getRowCount(), is(equalTo(3)));
		assertThat(table.getTableData(), hasKey("key1"));
		assertThat(table.getTableData(), hasKey("key2"));
		assertThat(table.getTableData(), hasKey("key3"));
	}
	
	@Test
	public void testForceSetInitialDataSetOverwritesExistingData() {
		KdbTable table = new KdbTable("my-test-table");
		table.setInitialDataSet(getTable());
		table.forceSetInitialDataSet(getTable2());
		
		assertThat(table.getRowCount(), is(equalTo(3)));
		assertThat(table.getTableData(), hasKey("key4"));
		assertThat(table.getTableData(), hasKey("key5"));
		assertThat(table.getTableData(), hasKey("key6"));
	}
	
	// KdbTable.addColumn
	
	@Test(expected=TableColumnAlreadyExistsException.class)
	public void testAddColumnThrowsExceptionIfColumnAlreadyExists() {
		KdbTable table = new KdbTable("my-test-table");
		table.setInitialDataSet(getTable());
		
		table.addColumn("key1", new ArrayList<>());
	}
	
	@Test(expected=TableSchemaMismatchException.class)
	public void testAddColumnThrowsExceptionIfColumnSizeDoesNotMatchTable() {
		KdbTable table = new KdbTable("my-test-table");
		table.setInitialDataSet(getTable());
		
		table.addColumn("key4", new ArrayList<>());
	}
	
	@Test
	public void testAddColumnAddsColumnToExistingData() {
		KdbTable table = new KdbTable("my-test-table");
		table.setInitialDataSet(getTable());
		
		List<Object> newColumn = ImmutableList.of("a", "b", "c");
		
		table.addColumn("key4", newColumn);
		
		assertThat(table.getRowCount(), is(equalTo(3)));
		assertThat(table.getTableData(), hasKey("key4"));
		assertThat(table.getRow(0).has("key4"), is(equalTo(true)));
	}
	
	// KdbTable.deleteColumn
	
	@Test(expected=IllegalArgumentException.class)
	public void testDeleteColumnThrowsExceptionIfNullColumn() {
		KdbTable table = new KdbTable("my-test-table");
		table.deleteColumn(null);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testDeleteColumnThrowsExceptionIfEmptyColumn() {
		KdbTable table = new KdbTable("my-test-table");
		table.deleteColumn("");
	}
	
	@Test
	public void testDeleteColumnDeletesColumn() {
		KdbTable table = new KdbTable("my-test-table");
		table.setInitialDataSet(getTable());
		table.deleteColumn("key1");
		
		assertThat(table.getTableData(), not(hasKey("key1")));
	}
	
	// KdbTable.addRow(KdbDict)
	
	@Test
	public void testAddRowKdbDictIgnoresNullRow() {
		KdbTable table = new KdbTable("my-test-table");
		table.addRow((KdbDict) null);
		
		assertThat(table.isEmpty(), is(equalTo(true)));
	}
	
	@Test
	public void testAddRowKdbDictAddsRow() {
		KdbTable table = new KdbTable("my-test-table");
		table.addRow(new KdbDict().add("col1", "val1"));
		
		assertThat(table.getTableData(), hasKey("col1"));
	}
	
	// KdbTable.addRow(Map)
	
	@Test
	public void testAddRowMapIgnoresNullAndEmptyMaps() {
		KdbTable table = new KdbTable("my-test-table");
		table.addRow((Map<String, Object>) null);
		table.addRow(new HashMap<>());
		
		assertThat(table.isEmpty(), is(equalTo(true)));
	}
	
	@Test(expected=TableSchemaMismatchException.class)
	public void testAddRowMapThrowsExceptionIfRowMissingColumn() {
		KdbTable table = new KdbTable("my-test-table", getTable());
		KdbDict newRow = new KdbDict()
									.add("key1", 1.3)
									.add("key2", 10);
		
		table.addRow(newRow);
	}
	
	@Test
	public void testAddRowMapAddsNewRow() {
		KdbTable table = new KdbTable("my-test-table", getTable());
		KdbDict newRow = new KdbDict()
									.add("key1", 1.3)
									.add("key2", 10)
									.add("key3", "a");
		
		table.addRow(newRow);
		
		assertThat(table.getRowCount(), is(equalTo(4)));
		assertThat(table.getRow(3), is(equalTo(newRow)));
	}
	
	@Test
	public void testAddRowMapAddsConvertsListAndEnumTypesCorrectly() {
		KdbTable table = new KdbTable("my-test-table");
		KdbDict newRow = new KdbDict()
									.add("nested-list", ImmutableList.of(1, 2, 3))
									.add("enum", TestEnum.VALUE_1);
		
		table.addRow(newRow);
		
		KdbDict addedRow = table.getRow(0);
		
		assertThat(addedRow.get("nested-list"), is(instanceOf(Object[].class)));
		assertThat(addedRow.get("enum"), is(equalTo("VALUE_1")));
	}
	
	// KdbTable.append
	
	@Test
	public void testAppendIgnoresNullOrEmptyTableToAdd() {
		KdbTable table = new KdbTable("my-test-table");
		table.append(null);
		table.append(new KdbTable("my-test-table"));;
		
		assertThat(table.isEmpty(), is(equalTo(true)));
	}
	
	@Test(expected=TableSchemaMismatchException.class)
	public void testAppendThrowsExceptionIfTableNameOfNonEmptyTableMismatches() {
		KdbTable table = new KdbTable("my-test-table");
		KdbTable toAppend = new KdbTable("bad-table-name", getTable());
		
		table.append(toAppend);
	}
	
	@Test(expected=TableSchemaMismatchException.class)
	public void testAppendThrowsExceptionIfTableSchemaToAppendMismatches() {
		KdbTable table = new KdbTable("my-test-table", getTable());
		KdbTable toAppend = new KdbTable("my-test-table", getTable2());
		
		table.append(toAppend);
	}
	
	@Test
	public void testAppendAppendsTableToEndOfTable() {
		KdbTable table = new KdbTable("my-test-table", getTable());
		KdbTable toAppend = new KdbTable("my-test-table", getTable());
		
		table.append(toAppend);
		
		assertThat(table.getRowCount(), is(equalTo(6)));
	}
	
	// KdbTable.getTableName
	
	@Test
	public void testGetTableNameReturnsTableName() {
		KdbTable table = new KdbTable("my-test-table");
		assertThat(table.getTableName(), is(equalTo("my-test-table")));
	}
	
	// KdbTable.getTableData
	
	@Test
	public void testGetTableDataReturnsUnderlyingTableData() {
		KdbTable table = new KdbTable("my-test-table");
		assertThat(table.getTableData(), is(anEmptyMap()));
	}
	
	// KdbTable.convertToFlip
	
	@Test
	public void testConvertToFlipReturnsNullForEmptyTable() {
		KdbTable table = new KdbTable("my-test-table");
		assertThat(table.convertToFlip(), is(nullValue()));
	}
	
	@Test
	public void testConvertToFlipReturnsFlip() {
		KdbTable table = new KdbTable("my-test-table", getTable());
		Flip converted = table.convertToFlip();
		
		assertThat(converted.x, is(arrayWithSize(3)));
		assertThat(converted.x, is(arrayContainingInAnyOrder("key1", "key2", "key3")));
	}
	
	// KdbTable.getRowCount()
	
	@Test
	public void testGetRowCountReturns0ForEmptyTable() {
		KdbTable table = new KdbTable("my-test-table");
		assertThat(table.getRowCount(), is(equalTo(0)));
	}
	
	@Test
	public void testGetRowCountReturnsRowCount() {
		KdbTable table = new KdbTable("my-test-table", getTable());
		assertThat(table.getRowCount(), is(equalTo(3)));
	}
	
	// KdbTable.isEmpty
	
	@Test
	public void testIsEmptyReturnsTrueForEmptyTable() {
		KdbTable table = new KdbTable("my-test-table");
		assertThat(table.isEmpty(), is(equalTo(false)));
	}
	
	@Test
	public void testIsEmptyReturnsFalseForNonEmptyTable() {
		KdbTable table = new KdbTable("my-test-table", getTable());
		assertThat(table.isEmpty(), is(equalTo(false)));
	}
	
	// KdbTable.changeTableName
	
	@Test(expected=UnsupportedOperationException.class)
	public void testChangeTableNameThrowsExceptionIfNewNameIsNull() {
		new KdbTable("table-name").changeTableName(null);
	}
	
	@Test
	public void testChangeTableNameChangesTableName() {
		KdbTable table = new KdbTable("table-name");
		table.changeTableName("some-other-table-name");
		
		assertThat(table.getTableName(), is(equalTo("some-other-table-name")));
	}
	
	
	private Flip getTable() {
		String[] keys = { "key1", "key2", "key3" };
		
		Object[] col1 = { 1.0, 1.1, 1.2 };
		Object[] col2 = { 7, 8, 9 };
		Object[] col3 = { "x", "y", "z" };
		
		Object[] cols = { col1, col2, col3 };
		
		return new Flip(new Dict(keys, cols));
	}
	
	private Flip getTable2() {
		String[] keys = { "key4", "key5", "key6" };
		
		Object[] col1 = { 1.0, 1.1, 1.2 };
		Object[] col2 = { 7, 8, 9 };
		Object[] col3 = { "x", "y", "z" };
		
		Object[] cols = { col1, col2, col3 };
		
		return new Flip(new Dict(keys, cols));
	}
	
	enum TestEnum {
		VALUE_1;
	}
}
