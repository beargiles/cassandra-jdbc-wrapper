package com.github.adejanovski.cassandra.jdbc.codec;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class ShortToIntCodec extends TypeCodec<Integer> {

    public ShortToIntCodec(Class<Integer> javaClass) {
        super(DataType.smallint(), javaClass);
    }

    @Override
    public ByteBuffer serialize(Integer paramT, ProtocolVersion paramProtocolVersion)
            throws InvalidTypeException {
        if (paramT == null) {
            return null;
        }
        return ByteBufferUtil.bytes(paramT);
    }

    @Override
    public Integer deserialize(ByteBuffer paramByteBuffer, ProtocolVersion paramProtocolVersion)
            throws InvalidTypeException {
        if (paramByteBuffer == null) {
            return null;

        }
        // always duplicate the ByteBuffer instance before consuming it!
        Short value = ByteBufferUtil.toShort(paramByteBuffer.duplicate());
        return value.intValue();
    }

    @Override
    public Integer parse(String paramString) throws InvalidTypeException {
        return Integer.valueOf(paramString);
    }

    @Override
    public String format(Integer paramT) throws InvalidTypeException {
        return String.valueOf(paramT);
    }

}
