package org.apache.ranger.plugin.resourcematcher;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by alal on 1/29/16.
 */
public class RangerAbstractResourceMatcherTest {

    @Test
    public void test_isAllPossibleValues() {
        RangerAbstractResourceMatcher matcher = new AbstractMatcherWrapper();
        for (String resource : new String[] { null, "", "*"}) {
            assertTrue(matcher.isAllValuesRequested(resource));
        }

        for (String resource : new String[] { " ", "\t", "\n", "foo"}) {
            assertFalse(matcher.isAllValuesRequested(resource));
        }
    }

    static class AbstractMatcherWrapper extends RangerAbstractResourceMatcher {

        @Override
        public boolean isMatch(String resource) {
            fail("This method is not expected to be used by test!");
            return false;
        }
    }
}