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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Types;

public class JdbcInetAddress extends AbstractJdbcType<InetAddress> {
    public static final JdbcInetAddress instance = new JdbcInetAddress();

    JdbcInetAddress() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(InetAddress obj) {
        return 0;
    }

    public int getPrecision(InetAddress obj) {
        // max size for inet4 is 'xxx:xxx:xxx:xxx'
        // max size for inet6 is much longer but still rare
        return (obj == null) ? 15 : obj.toString().length();
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(InetAddress obj) {
        return (obj == null) ? null : obj.getHostAddress();
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        return compose(bytes).getHostAddress();
    }

    public Class<InetAddress> getType() {
        return InetAddress.class;
    }

    public int getJdbcType() {
        return Types.OTHER;
    }

    public InetAddress compose(Object value) {
        try {
            return InetAddress.getByName((String) value);
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    public Object decompose(InetAddress value) {
        return value.getAddress();
    }
}
