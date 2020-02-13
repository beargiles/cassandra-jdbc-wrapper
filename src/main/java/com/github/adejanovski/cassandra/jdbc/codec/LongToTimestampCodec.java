package com.github.adejanovski.cassandra.jdbc.codec;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class LongToTimestampCodec extends TypeCodec<Timestamp> {

    public LongToTimestampCodec(Class<Timestamp> javaClass) {
        super(DataType.bigint(), javaClass);
    }

    @Override
    public ByteBuffer serialize(Timestamp paramT, ProtocolVersion paramProtocolVersion)
            throws InvalidTypeException {
        if (paramT == null) {
            return null;
        }
        return ByteBufferUtil.bytes(paramT.getTime());
    }

    @Override
    public Timestamp deserialize(ByteBuffer paramByteBuffer, ProtocolVersion paramProtocolVersion)
            throws InvalidTypeException {
        if (paramByteBuffer == null) {
            return null;

        }
        // always duplicate the ByteBuffer instance before consuming it!
        return new Timestamp(ByteBufferUtil.toLong(paramByteBuffer.duplicate()));
    }

    @Override
    public Timestamp parse(String paramString) throws InvalidTypeException {
        return Timestamp.valueOf(paramString);
    }

    @Override
    public String format(Timestamp paramT) throws InvalidTypeException {
        return String.valueOf(paramT);
    }

}
