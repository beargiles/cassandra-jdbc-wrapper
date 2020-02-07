package com.github.adejanovski.cassandra.jdbc.codec;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class IntToShortCodec extends TypeCodec<Short> {

	public IntToShortCodec(Class<Short> javaClass) {
		super(DataType.cint(), javaClass);
	}

	@Override
	public ByteBuffer serialize(Short paramT, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
		if (paramT == null) {
			return null;
		}
		return ByteBufferUtil.bytes(paramT.shortValue());
	}

	@Override
	public Short deserialize(ByteBuffer paramByteBuffer, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
		if (paramByteBuffer == null) {
			return null;

		}
		// always duplicate the ByteBuffer instance before consuming it!
		return ByteBufferUtil.toShort(paramByteBuffer.duplicate());
	}

	@Override
	public Short parse(String paramString) throws InvalidTypeException {
		return Short.valueOf(paramString);
	}

	@Override
	public String format(Short paramT) throws InvalidTypeException {
		return String.valueOf(paramT);
	}
}
