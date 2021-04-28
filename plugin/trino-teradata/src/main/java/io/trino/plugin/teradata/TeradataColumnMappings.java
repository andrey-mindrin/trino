/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DoubleWriteFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.VarcharType;

import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.Types;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.PredicatePushdownController.CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;

public final class TeradataColumnMappings
{
    private TeradataColumnMappings() {}

    public static ColumnMapping tinyintColumnMapping()
    {
        return ColumnMapping.longMapping(TINYINT, ResultSet::getByte, tinyintWriteFunction());
    }

    public static ColumnMapping smallintColumnMapping()
    {
        return ColumnMapping.longMapping(SMALLINT, ResultSet::getShort, smallintWriteFunction());
    }

    public static ColumnMapping integerColumnMapping()
    {
        return ColumnMapping.longMapping(INTEGER, ResultSet::getInt, integerWriteFunction());
    }

    public static ColumnMapping bigintColumnMapping()
    {
        return ColumnMapping.longMapping(BIGINT, ResultSet::getLong, bigintWriteFunction());
    }

    public static ColumnMapping realColumnMapping()
    {
        return ColumnMapping.longMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)), realWriteFunction());
    }

    public static ColumnMapping doubleColumnMapping()
    {
        return ColumnMapping.doubleMapping(DOUBLE, ResultSet::getDouble, doubleWriteFunction());
    }

    public static ColumnMapping decimalColumnMapping(DecimalType decimalType)
    {
        return decimalColumnMapping(decimalType, UNNECESSARY);
    }

    public static ColumnMapping decimalColumnMapping(DecimalType decimalType, RoundingMode roundingMode)
    {
        if (decimalType.isShort()) {
            checkArgument(roundingMode == UNNECESSARY, "Round mode is not supported for short decimal, map the type to long decimal instead");
            return ColumnMapping.longMapping(
                    decimalType,
                    shortDecimalReadFunction(decimalType),
                    shortDecimalWriteFunction(decimalType));
        }
        return ColumnMapping.sliceMapping(
                decimalType,
                longDecimalReadFunction(decimalType, roundingMode),
                longDecimalWriteFunction(decimalType));
    }

    public static ColumnMapping charColumnMapping(CharType charType, boolean isRemoteCaseSensitive)
    {
        requireNonNull(charType, "charType is null");
        PredicatePushdownController pushdownController = isRemoteCaseSensitive ? FULL_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(charType, charReadFunction(charType), charWriteFunction(), pushdownController);
    }

    public static ColumnMapping varcharColumnMapping(VarcharType varcharType, boolean isRemoteCaseSensitive)
    {
        PredicatePushdownController pushdownController = isRemoteCaseSensitive ? FULL_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), pushdownController);
    }

    public static ColumnMapping defaultCharColumnMapping(int columnSize, boolean isRemoteCaseSensitive)
    {
        if (columnSize > CharType.MAX_LENGTH) {
            return defaultVarcharColumnMapping(columnSize, isRemoteCaseSensitive);
        }
        return charColumnMapping(createCharType(columnSize), isRemoteCaseSensitive);
    }

    public static ColumnMapping defaultVarcharColumnMapping(int columnSize, boolean isRemoteCaseSensitive)
    {
        if (columnSize > VarcharType.MAX_LENGTH) {
            return varcharColumnMapping(createUnboundedVarcharType(), isRemoteCaseSensitive);
        }
        return varcharColumnMapping(createVarcharType(columnSize), isRemoteCaseSensitive);
    }

    public static ColumnMapping dateColumnMapping()
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunction(),
                dateWriteFunction());
    }

    public static LongWriteFunction tinyintWriteFunction()
    {
        return TeradataLongWriteFunction.writeFunction(Types.TINYINT, StandardColumnMappings.tinyintWriteFunction());
    }

    public static LongWriteFunction smallintWriteFunction()
    {
        return TeradataLongWriteFunction.writeFunction(Types.SMALLINT, StandardColumnMappings.smallintWriteFunction());
    }

    public static LongWriteFunction integerWriteFunction()
    {
        return TeradataLongWriteFunction.writeFunction(Types.INTEGER, StandardColumnMappings.integerWriteFunction());
    }

    public static LongWriteFunction bigintWriteFunction()
    {
        return TeradataLongWriteFunction.writeFunction(Types.BIGINT, StandardColumnMappings.bigintWriteFunction());
    }

    public static LongWriteFunction realWriteFunction()
    {
        return TeradataLongWriteFunction.writeFunction(Types.REAL, StandardColumnMappings.realWriteFunction());
    }

    public static DoubleWriteFunction doubleWriteFunction()
    {
        return TeradataDoubleWriteFunction.writeFunction(Types.DOUBLE, StandardColumnMappings.doubleWriteFunction());
    }

    public static LongWriteFunction shortDecimalWriteFunction(DecimalType decimalType)
    {
        return TeradataLongWriteFunction.writeFunction(Types.DECIMAL, StandardColumnMappings.shortDecimalWriteFunction(decimalType));
    }

    public static SliceWriteFunction longDecimalWriteFunction(DecimalType decimalType)
    {
        return TeradataSliceWriteFunction.writeFunction(Types.DECIMAL, StandardColumnMappings.longDecimalWriteFunction(decimalType));
    }

    public static SliceWriteFunction charWriteFunction()
    {
        return TeradataSliceWriteFunction.writeFunction(Types.CHAR, StandardColumnMappings.charWriteFunction());
    }

    public static SliceWriteFunction varcharWriteFunction()
    {
        return TeradataSliceWriteFunction.writeFunction(Types.VARCHAR, StandardColumnMappings.varcharWriteFunction());
    }

    public static SliceWriteFunction varbinaryWriteFunction()
    {
        return TeradataSliceWriteFunction.writeFunction(Types.VARBINARY, StandardColumnMappings.varbinaryWriteFunction());
    }

    public static LongWriteFunction dateWriteFunction()
    {
        return TeradataLongWriteFunction.writeFunction(Types.DATE, StandardColumnMappings.dateWriteFunction());
    }
}
