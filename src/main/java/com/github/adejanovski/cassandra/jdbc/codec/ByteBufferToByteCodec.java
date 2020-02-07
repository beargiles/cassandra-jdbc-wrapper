package com.github.adejanovski.cassandra.jdbc.codec;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class ByteBufferToByteCodec extends TypeCodec<Byte> {

	public ByteBufferToByteCodec(Class<Byte> javaClass) {
		super(DataType.tinyint(), javaClass);
	}

	@Override
	public ByteBuffer serialize(Byte paramT, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
		if (paramT == null) {
			return null;
		}
		return ByteBufferUtil.bytes(paramT.byteValue());
	}

	@Override
	public Byte deserialize(ByteBuffer paramByteBuffer, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
		if (paramByteBuffer == null) {
			return null;

		}
		// always duplicate the ByteBuffer instance before consuming it!
		return paramByteBuffer.duplicate().get();
	}

	@Override
	public Byte parse(String paramString) throws InvalidTypeException {
		return Byte.valueOf(paramString);
	}

	@Override
	public String format(Byte paramT) throws InvalidTypeException {
		return String.valueOf(paramT);
	}
}
