package com.github.adejanovski.cassandra.jdbc.codec;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class ByteToByteBufferCodec extends TypeCodec<ByteBuffer> {

	public ByteToByteBufferCodec(Class<ByteBuffer> javaClass) {
		super(DataType.blob(), javaClass);
	}

	@Override
	public ByteBuffer serialize(ByteBuffer paramT, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
		if (paramT == null) {
			return null;
		}
		return ByteBufferUtil.bytes(paramT.duplicate().get());
	}

	@Override
	public ByteBuffer deserialize(ByteBuffer paramByteBuffer, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
		if (paramByteBuffer == null) {
			return null;

		}
		// always duplicate the ByteBuffer instance before consuming it!
        return ByteBufferUtil.bytes(Byte.valueOf(paramByteBuffer.duplicate().get()));
	}

	@Override
	public ByteBuffer parse(String paramString) throws InvalidTypeException {
	    return ByteBufferUtil.bytes(Byte.valueOf(paramString));
	}

	@Override
	public String format(ByteBuffer paramT) throws InvalidTypeException {
		return String.valueOf(paramT);
	}

}
