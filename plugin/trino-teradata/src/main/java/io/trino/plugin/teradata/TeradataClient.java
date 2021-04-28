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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.bigintColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.bigintWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.charWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.dateColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.dateWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.decimalColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.doubleColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.doubleWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.integerColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.integerWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.realColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.realWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.smallintColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.smallintWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.teradata.TeradataColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.teradata.TeradataColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class TeradataClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(TeradataClient.class);
    private static final int TERADATA_VARCHAR_MAX_CHARS = 64000;

    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BIGINT, WriteMapping.longMapping("bigint", bigintWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("smallint", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("tinyint", tinyintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("integer", integerWriteFunction()))
            .put(DOUBLE, WriteMapping.doubleMapping("double precision", doubleWriteFunction()))
            .put(VARBINARY, WriteMapping.sliceMapping("blob", varbinaryWriteFunction()))
            .put(DATE, WriteMapping.longMapping("date", dateWriteFunction()))
            .put(REAL, WriteMapping.longMapping("real", realWriteFunction()))
            .build();

    @Inject
    public TeradataClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            TypeManager typeManager)
    {
        super(config, "\"", connectionFactory);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.getJdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                int precision = typeHandle.getRequiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                // TODO does mysql support negative scale?
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(dateColumnMapping());

            case Types.TIMESTAMP:
                // TODO support higher precisions (https://github.com/trinodb/trino/issues/6910)
                break; // currently handled by the default mappings
        }
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType) {
            String dataType;
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > TERADATA_VARCHAR_MAX_CHARS) {
                dataType = "clob";
                return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
                return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
            }
        }
        if (type instanceof CharType) {
            String dataType = "char(" + ((CharType) type).getLength() + ")";
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }
        if (type instanceof DecimalType) {
            String dataType = format("number(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
            if (((DecimalType) type).isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction((DecimalType) type));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction((DecimalType) type));
        }
        if (type instanceof DoubleType) {
            int t = 1;
        }
        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(ImmutableList.of("TABLE", "VIEW", "SYSTEM TABLE"));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql.replace("SELECT", "SELECT TOP " + limit));
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "CREATE MULTISET TABLE %s AS (SELECT %s FROM %s) WITH NO DATA",
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName));
        execute(connection, sql);
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newSchemaName = newSchemaName.toUpperCase(ENGLISH);
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "RENAME TABLE %s AS %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(catalogName, newSchemaName, newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return format("CREATE MULTISET TABLE %s (%s)", quoted(remoteTableName), join(", ", columns));
    }
}
