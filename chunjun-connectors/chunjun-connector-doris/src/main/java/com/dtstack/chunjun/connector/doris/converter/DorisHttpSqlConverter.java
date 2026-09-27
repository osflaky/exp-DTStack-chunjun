/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.doris.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.doris.buffer.DorisSinkOP;
import com.dtstack.chunjun.connector.doris.options.DorisConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class DorisHttpSqlConverter
        extends AbstractRowConverter<RowData, RowData, Map<String, Object>, LogicalType> {

    private final boolean enableDelete;
    private final List<String> columnList;
    public static final String DATETIME_FORMAT_SHORT = "yyyy-MM-dd HH:mm:ss";

    public DorisHttpSqlConverter(RowType rowType, CommonConfig conf) {
        super(rowType, conf);
        this.columnList = rowType.getFieldNames();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
        this.enableDelete = ((DorisConfig) conf).isEnableDelete();
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> wrapIntoNullableExternalConverter(
            ISerializationConverter<Map<String, Object>> ISerializationConverter,
            LogicalType type) {
        return (rowData, index, output) -> {
            if (rowData == null
                    || rowData.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                output.put(columnList.get(index), null);
            } else {
                ISerializationConverter.serialize(rowData, index, output);
            }
        };
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        throw new UnsupportedOperationException("Not support toInternal converter");
    }

    @Override
    public Map<String, Object> toExternal(RowData rowData, Map<String, Object> output)
            throws Exception {
        for (int index = 0; index < toExternalConverters.size(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, output);
        }
        if (enableDelete) {
            output.put(DorisSinkOP.COLUMN_KEY, DorisSinkOP.parse(rowData.getRowKind()));
        }

        return output;
    }

    @Override
    protected IDeserializationConverter<Object, AbstractBaseColumn> createInternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> new BooleanColumn((Boolean) val);
            case TINYINT:
                return val -> new ByteColumn((byte) val);
            case SMALLINT:
                return val -> new ShortColumn((short) val);
            case INTEGER:
                return val -> new IntColumn((int) val);
            case BIGINT:
                return val -> new LongColumn((long) val);
            case FLOAT:
                return val -> new FloatColumn((float) val);
            case DOUBLE:
                return val -> new DoubleColumn((double) val);
            case DECIMAL:
                return val -> new BigDecimalColumn((BigDecimal) val);
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case DATE:
                return val -> new SqlDateColumn(Date.valueOf((String) val));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return val ->
                        new TimestampColumn(Timestamp.valueOf((String) val), timestampPrecision);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getBoolean(index) ? 1 : 0);
            case TINYINT:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getByte(index));
            case SMALLINT:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getShort(index));
            case INTEGER:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getInt(index));
            case BIGINT:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getLong(index));
            case FLOAT:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getFloat(index));
            case DOUBLE:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getDouble(index));
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (rowData, index, map) ->
                        map.put(
                                columnList.get(index),
                                rowData.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            case CHAR:
            case VARCHAR:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getString(index).toString());

            case DATE:
                return (rowData, index, map) ->
                        map.put(
                                columnList.get(index),
                                Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(index)))
                                        .toString());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, index, map) -> {
                    final int timestampPrecision = ((TimestampType) type).getPrecision();
                    map.put(
                            columnList.get(index),
                            rowData.getTimestamp(index, timestampPrecision)
                                    .toTimestamp()
                                    .toString());
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (rowData, index, map) -> {
                    final int localP = ((LocalZonedTimestampType) type).getPrecision();
                    map.put(
                            columnList.get(index),
                            rowData.getTimestamp(index, localP).toTimestamp().toString());
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static String addStrForNum(String str, int strLength, String appendStr) {
        int strLen = str.length();
        if (strLen < strLength) {
            while (strLen < strLength) {
                StringBuffer sb = new StringBuffer();
                sb.append(str).append(appendStr);
                str = sb.toString();
                strLen = str.length();
            }
        }
        return str;
    }
}
