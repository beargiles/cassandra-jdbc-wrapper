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

import java.sql.Types;



public class JdbcShort extends AbstractJdbcType<Short>
{
    public static final JdbcShort instance = new JdbcShort();

    JdbcShort() {}

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(Short obj)
    {
        return 0;
    }

    public int getPrecision(Short obj)
    {
        return obj.toString().length();
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return true;
    }

    public String toString(Short obj)
    {
        return obj.toString();
    }

    public boolean needsQuotes()
    {
        return false;
    }

    public Class<Short> getType()
    {
        return Short.class;
    }

    public int getJdbcType()
    {
        return Types.SMALLINT;
    }

    public Short compose(Object obj)
    {
        return (Short)obj;
    }

    public Object decompose(Short value)
    {
        return (Object)value;
    }
}
