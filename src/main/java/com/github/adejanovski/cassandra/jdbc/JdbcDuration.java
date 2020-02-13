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
import java.text.SimpleDateFormat;
import java.util.Date;

// FIXME - ...
// Durations are internally 3 ints. CqlDuration?
public class JdbcDuration extends AbstractJdbcType<Date> {
    public static final String[] iso8601Patterns = new String[] { "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mmZ", "yyyy-MM-dd HH:mm:ssZ",
            "yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd'T'HH:mmZ", "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd", "yyyy-MM-ddZ", "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ" };
    static final String DEFAULT_FORMAT = iso8601Patterns[3];
    static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat(DEFAULT_FORMAT);
        }
    };

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
        return FORMATTER.get().format(obj);
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if (bytes.remaining() == 0) {
            return "";
        }
        if (bytes.remaining() != 8) {
            throw new MarshalException(
                    "A date is exactly 8 bytes (stored as a long): " + bytes.remaining());
        }

        // uses ISO-8601 formatted string
        return FORMATTER.get().format(new Date(bytes.getLong(bytes.position())));
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
