package com.github.adejanovski.cassandra.jdbc.codec;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class BigIntegerToIntCodec extends TypeCodec<BigInteger> {

	public BigIntegerToIntCodec(Class<BigInteger> javaClass) {
		super(DataType.cint(), javaClass);
	}

	@Override
	public ByteBuffer serialize(BigInteger paramT, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
		if (paramT == null) {
			return null;
		}
		return ByteBuffer.wrap(paramT.toByteArray());
	}

	@Override
	public BigInteger deserialize(ByteBuffer paramByteBuffer, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
		if (paramByteBuffer == null) {
			return null;

		}
		// always duplicate the ByteBuffer instance before consuming it!
		return new BigInteger(ByteBufferUtil.getArray(paramByteBuffer.duplicate()));
	}

	@Override
	public BigInteger parse(String paramString) throws InvalidTypeException {
		return new BigInteger(paramString);
	}

	@Override
	public String format(BigInteger paramT) throws InvalidTypeException {
		return String.valueOf(paramT);
	}

}
