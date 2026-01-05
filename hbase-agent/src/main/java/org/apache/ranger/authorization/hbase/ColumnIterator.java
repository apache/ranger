/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.authorization.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class ColumnIterator implements Iterator<String> {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnIterator.class.getName());

    Iterator<byte[]> setIterator;
    Iterator<Cell>   listIterator;

    @SuppressWarnings("unchecked")
    public ColumnIterator(Collection<?> columnCollection) {
        if (columnCollection != null) {
            if (columnCollection instanceof Set) {
                setIterator = ((Set<byte[]>) columnCollection).iterator();
            } else if (columnCollection instanceof List) {
                listIterator = ((List<Cell>) columnCollection).iterator();
            } else { // unexpected
                // TODO make message better
                LOG.error("Unexpected type {} passed as value in column family collection", columnCollection.getClass().getName());
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (setIterator != null) {
            return setIterator.hasNext();
        } else if (listIterator != null) {
            return listIterator.hasNext();
        } else {
            return false;
        }
    }

    /**
     * Never returns a null value.  Will return empty string in case of null value.
     */
    @Override
    public String next() {
        final String value;

        if (setIterator != null) {
            byte[] valueBytes = setIterator.next();

            value = (valueBytes != null) ? Bytes.toString(valueBytes) : "";
        } else if (listIterator != null) {
            Cell   cell = listIterator.next();
            byte[] v    = CellUtil.cloneQualifier(cell);

            value = Bytes.toString(v);
        } else {
            // TODO make the error message better
            throw new NoSuchElementException("Empty values passed in!");
        }

        return value;
    }

    @Override
    public void remove() {
        // TODO make the error message better
        throw new UnsupportedOperationException("Remove not supported from iterator!");
    }
}
