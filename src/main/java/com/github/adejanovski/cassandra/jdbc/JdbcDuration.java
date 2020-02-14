/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.github.adejanovski.cassandra.jdbc;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Date;

// FIXME - ...
// Durations are internally 3 ints. CqlDuration?
public class JdbcDuration extends AbstractJdbcType<Date> {

    public static final JdbcDuration instance = new JdbcDuration();

    JdbcDuration() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(Date obj) {
        return -1;
    }

    public int getPrecision(Date obj) {
        return -1;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return false;
    }

    public String toString(Date obj) {
        return "UNIMPLEMENTED";
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        } else if (bytes.remaining() != 8) {
            throw new MarshalException(
                    "A date is exactly 8 bytes (stored as a long): " + bytes.remaining());
        }

        // uses ISO-8601 formatted string
        return "UNIMPLEMENTED";
    }

    public Class<Date> getType() {
        return Date.class;
    }

    public int getJdbcType() {
        return Types.OTHER;
    }

    public Date compose(Object value) {
        return (Date) value;
    }

    public Object decompose(Date value) {
        return (value == null) ? null : (Object) value;
    }

}
