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

import io.trino.plugin.jdbc.DoubleWriteFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class TeradataDoubleWriteFunction
        implements DoubleWriteFunction
{
    private final int sqlType;
    private final DoubleWriteFunction writeFunction;

    private TeradataDoubleWriteFunction(int sqlType, DoubleWriteFunction writeFunction)
    {
        this.sqlType = sqlType;
        this.writeFunction = requireNonNull(writeFunction, "writeFunction is null");
    }

    public static TeradataDoubleWriteFunction writeFunction(int dataType, DoubleWriteFunction writeFunction)
    {
        return new TeradataDoubleWriteFunction(dataType, writeFunction);
    }

    @Override
    public void setNull(PreparedStatement statement, int index)
            throws SQLException
    {
        statement.setNull(index, sqlType);
    }

    @Override
    public void set(PreparedStatement statement, int index, double value)
            throws SQLException
    {
        writeFunction.set(statement, index, value);
    }
}
