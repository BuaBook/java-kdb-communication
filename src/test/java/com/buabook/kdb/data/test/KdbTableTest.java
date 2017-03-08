package com.buabook.kdb.data.test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

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
}
