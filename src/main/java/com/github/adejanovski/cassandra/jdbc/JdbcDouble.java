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

public class JdbcDouble extends AbstractJdbcType<Double> {
    public static final JdbcDouble instance = new JdbcDouble();

    JdbcDouble() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(Double obj) {
        return 300;
    }

    public int getPrecision(Double obj) {
        return 15;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(Double obj) {
        return (obj == null) ? null : obj.toString();
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        } else if (bytes.remaining() != 8) {
            throw new MarshalException(
                    "A double is exactly 8 bytes: " + bytes.remaining());
        }

        return toString(bytes.getDouble(bytes.position()));
    }

    public Class<Double> getType() {
        return Double.class;
    }

    public int getJdbcType() {
        return Types.DOUBLE;
    }

    public Double compose(Object value) {
        return (Double) value;
    }

    public Object decompose(Double value) {
        return value;
    }
}
