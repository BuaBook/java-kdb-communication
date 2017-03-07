package com.buabook.kdb.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import com.buabook.kdb.Flips;
import com.buabook.kdb.data.KdbTable;
import com.kx.c.Dict;
import com.kx.c.Flip;

public class FlipsTest {

	// Flips.getRowCount
	
	@Test
	public void testGetRowCountReturnsZeroForNullTable() {
		assertThat(Flips.getRowCount(null), is(equalTo(0)));
	}
	
	@Test
	public void testGetRowCountReturnsZeroForEmptyTable() {
		Flip table = new Flip(new Dict(null, null));
		
		assertThat(Flips.getRowCount(table), is(equalTo(0)));
	}
	
	@Test
	public void testGetRowCountReturnsRowCountForNonEmptyTable() {
		assertThat(Flips.getRowCount(getTable()), is(equalTo(3)));
	}
	
	// Flips.getColumn
	
	@Test
	public void testGetColumnReturnsNullForNullTable() {
		assertThat(Flips.getColumn(null, 0), is(nullValue()));
	}
	
	@Test
	public void testGetColumnReturnsNullForNullColumn() {
		assertThat(Flips.getColumn(getTable(), null), is(nullValue()));
	}
	
	@Test
	public void testGetColumnReturnsColumnForTable() {
		Object[] column = Flips.getColumn(getTable(), 0);
		
		assertThat(column, is(not(nullValue())));
		assertThat(column, is(arrayWithSize(3)));
		assertThat(column[0], is(instanceOf(Double.class)));
	}
	
	// Flips.isNullOrEmpty(KdbTable)
	
	@Test
	public void testIsNullOrEmptyKdbTableReturnsTrueForNullTable() {
		assertThat(Flips.isNullOrEmpty((KdbTable) null), is(equalTo(true)));
	}
	
	@Test
	public void testIsNullOrEmptyKdbTableReturnsTrueForEmptyTable() {
		KdbTable table = new KdbTable("test-table");
		
		assertThat(Flips.isNullOrEmpty(table), is(equalTo(true)));
	}
	
	@Test
	public void testIsNullOrEmptyKdbTableReturnsFalseForNonEmptyTable() {
		KdbTable table = new KdbTable("test-table", getTable());
		
		assertThat(Flips.isNullOrEmpty(table), is(equalTo(false)));
	}
	
	// Flips.isNullOrEmpty(Flip)
	
	@Test
	public void testIsNullOrEmptyFlipReturnsTrueForNullTable() {
		assertThat(Flips.isNullOrEmpty((Flip) null), is(equalTo(true)));
	}
	
	@Test
	public void testIsNullOrEmptyFlipReturnsTrueForEmptyTable() {
		Flip table = new Flip(new Dict(null, null));
		
		assertThat(Flips.isNullOrEmpty(table), is(equalTo(true)));
	}
	
	@Test
	public void testIsNullOrEmptyFlipReturnsFalseForEmptyTable() {
		assertThat(Flips.isNullOrEmpty(getTable()), is(equalTo(false)));
	}
	
	
	private Flip getTable() {
		String[] keys = { "key1", "key2", "key3" };
		
		Object[] col1 = { 1.0, 1.1, 1.2 };
		Object[] col2 = { 7, 8, 9 };
		Object[] col3 = { "x", "y", "z" };
		
		Object[] cols = { col1, col2, col3 };
		
		return new Flip(new Dict(keys, cols));
	}
}
