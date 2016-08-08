package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class ArraySortGroupTest {

    @Test
    public void simple() {
        long[] data = new long[] { //
                1, 2, 3, 4, //
                5, 6, 3, 1, //
                4, 8, 1, 2, //
                4, 5, 12, 10, //
                3, 3, 3, 3, //
                4, 3, 1, 2, //
                3, 3, 3, 3, //
        };

        long[] expectedSorted = new long[] { //
                1, 2, 3, 4, //
                3, 3, 3, 3, //
                3, 3, 3, 3, //
                4, 3, 1, 2, //
                4, 5, 12, 10, //
                4, 8, 1, 2, //
                5, 6, 3, 1, //
        };

        ArrayGroupSort sorter = new ArrayGroupSort(2, 4);
        sorter.sort(data);

        assertArrayEquals(expectedSorted, data);
    }

    @Test
    public void errors() {
        try {
            new ArrayGroupSort(3, 2);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            new ArrayGroupSort(-1, 2);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            new ArrayGroupSort(1, -1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        ArrayGroupSort sorter = new ArrayGroupSort(1, 3);

        try {
            sorter.sort(new long[] { 1, 2, 3, 4 });
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            sorter.sort(new long[] { 1, 2 });
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }
}
