using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CopyDatabase
{
    class TableSync
    {
        public string SourceConnectionString { get; set; }
        public string SourceSchema { get; set; }
        public string SourceFilter { get; set; }
        public string TargetConnectionString { get; set; }
        public string TargetSchema { get; set; }
        public string Table { get; set; }
        public TimeSpan WaitBetweenMergeCommands { get; set; }

        private Dictionary<string, string> columnDataTypes;
        private List<string> autoincrementColumnsList;
        private string orderByColumns;
        private string insertCmdText;
        private string updateCmdText;

        private async Task LoadSourceTableMetadataAsync()
        {
            SqlCommand command = null;
            using (var sourceConn = new SqlConnection(this.SourceConnectionString))
            {
                await sourceConn.OpenAsync();
                var primaryKeysList = new List<string>();
                command = sourceConn.CreateCommand();
                command.CommandText = $"select column_name from information_schema.key_column_usage where OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 and table_name='{this.Table}' and table_schema='{this.SourceSchema}' order by ordinal_position";
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        primaryKeysList.Add(reader.GetString(0));
                    }
                }
                if (primaryKeysList.Count == 0)
                {
                    throw new Exception($"{this.Table} has no primary keys");
                }

                autoincrementColumnsList = new List<string>();
                command = sourceConn.CreateCommand();
                command.CommandText = $"select name from sys.columns where is_identity=1 and object_id = object_id('{this.SourceSchema}.{this.Table}') order by column_id";
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        autoincrementColumnsList.Add(reader.GetString(0));
                    }
                }

                orderByColumns = string.Join(',', primaryKeysList.Select(row => $"[{row}]"));
                //Console.WriteLine("  {0} orderByColumns: {1} autoIncrementColumns: {2}", this.Table, orderByColumns, string.Join(',', autoincrementColumnsList.ToArray()));

                columnDataTypes = new Dictionary<string, string>();
                command = sourceConn.CreateCommand();
                command.CommandText = $"select top 1 * from [{this.SourceSchema}].[{this.Table}]";
                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (columnDataTypes.Count == 0)
                    {
                        var schema = reader.GetSchemaTable();
                        foreach (DataRow row in schema.Rows)
                        {
                            columnDataTypes.Add((string)row["ColumnName"], (string)row["DataTypeName"]);
                        }
                    }

                    while (reader.Read())
                    {
                        insertCmdText = $"insert into [{this.TargetSchema}].[{this.Table}] (";
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (!autoincrementColumnsList.Contains(reader.GetName(i)))
                            {
                                insertCmdText += $"[{reader.GetName(i)}],";
                            }
                        }
                        insertCmdText = insertCmdText.TrimEnd(',') + ") values (";
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (!autoincrementColumnsList.Contains(reader.GetName(i)))
                            {
                                insertCmdText += $"@{reader.GetName(i)},";
                            }
                        }
                        insertCmdText = insertCmdText.TrimEnd(',') + ")";
                        //Console.WriteLine($"  {insertCmdText}");

                        updateCmdText = $"update [{this.TargetSchema}].[{this.Table}] set ";
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (!"Key".Equals(reader.GetName(i), StringComparison.InvariantCultureIgnoreCase) && !"EffectiveDateUtc".Equals(reader.GetName(i), StringComparison.InvariantCultureIgnoreCase))
                            {
                                updateCmdText += $"[{reader.GetName(i)}]=@{reader.GetName(i)},";
                            }
                        }

                        if (columnDataTypes.ContainsKey("Key") && columnDataTypes.ContainsKey("EffectiveDateUtc") && columnDataTypes.ContainsKey("LastUpdateUtc"))
                        {
                            updateCmdText = updateCmdText.TrimEnd(',') + " where [Key]=@Key and EffectiveDateUtc=@EffectiveDateUtc and [LastUpdateUtc]<@LastUpdateUtc";
                        }
                        else
                        {
                            updateCmdText = updateCmdText.TrimEnd(',') + " where 1=1";
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                if (primaryKeysList.Contains(reader.GetName(i)))
                                {
                                    updateCmdText += $" and [{reader.GetName(i)}]=@{reader.GetName(i)}";
                                }
                            }
                        }
                        //Console.WriteLine($"  {updateCmdText}");
                    }
                }
            }
        }

        private async Task BruteForceCopyAsync()
        {
            var rows = 0;
            var inserts = 0;
            var updates = 0;
            var errors = 0;
            var page = -1;
            var rowsPerPage = 1000;
            var watch = new Stopwatch();
            while (true)
            {
                using (var sourceConn = new SqlConnection(this.SourceConnectionString))
                {
                    await sourceConn.OpenAsync();
                    page++;
                    var commandSource = sourceConn.CreateCommand();
                    commandSource.CommandText = $"select * from [{this.SourceSchema}].[{this.Table}] where 1=1 {this.SourceFilter} order by {orderByColumns} offset ({page}*{rowsPerPage}) rows fetch next {rowsPerPage} rows only";
                    using (var reader = await commandSource.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows)
                        {
                            break;
                        }
                        while (reader.Read())
                        {
                            if (rows % 1000 == 0)
                            {
                                Console.WriteLine($"{this.Table} rows: {rows} time: {DateTime.UtcNow.ToString("HH:mm:ss")}");
                            }
                            rows++;
                            using (var targetConn = new SqlConnection(this.TargetConnectionString))
                            {
                                await targetConn.OpenAsync();
                                var insertCommand = targetConn.CreateCommand();
                                insertCommand.CommandTimeout = 300;
                                AddParameters(reader, insertCommand);
                                insertCommand.CommandText = insertCmdText;
                                try
                                {
                                    watch.Restart();
                                    var insertCount = await insertCommand.ExecuteNonQueryAsync();
                                    watch.Stop();
                                    inserts += insertCount;
                                    Console.WriteLine($"  {this.Table} inserted {insertCount} rows in {watch.Elapsed} with {GetSqlKeys(insertCommand)}");
                                }
                                catch (Exception insertException)
                                {
                                    if (!insertException.Message.Contains("Cannot insert duplicate key"))
                                    {
                                        errors += 1;
                                        Console.WriteLine($"  {this.Table} {GetSqlKeys(insertCommand)} {insertException.GetType()}: {insertException.Message}");
                                        continue;
                                    }

                                    var updateCommand = targetConn.CreateCommand();
                                    updateCommand.CommandTimeout = 300;
                                    AddParameters(reader, updateCommand);
                                    updateCommand.CommandText = updateCmdText;
                                    try
                                    {
                                        watch.Restart();
                                        var updateCount = await updateCommand.ExecuteNonQueryAsync();
                                        watch.Stop();
                                        updates += updateCount;
                                        Console.WriteLine($"  {this.Table} updated {updateCount} rows in {watch.Elapsed} with {GetSqlKeys(updateCommand)}");
                                    }
                                    catch (Exception updateException)
                                    {
                                        errors += 1;
                                        Console.WriteLine($"  {this.Table} {GetSqlKeys(updateCommand)} {updateException.GetType()}: {updateException.Message}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Console.WriteLine($"{this.Table} rows: {rows} inserts:{inserts}, updates:{updates}, errors:{errors}");
        }
        private async Task SmartCopyAsync()
        {
            var rows = 0;
            var inserts = 0;
            var updates = 0;
            var deletes = 0;
            var errors = 0;
            var page = -1;
            var rowsPerPage = 1000;
            var watch = new Stopwatch();
            while (true)
            {
                var sourceActions = new Dictionary<string, TableColumnKeys>();
                var targetActions = new Dictionary<string, TableColumnKeys>();
                string minKey = null;
                string maxKey = null;
                using (var sourceConn = new SqlConnection(this.SourceConnectionString))
                {
                    await sourceConn.OpenAsync();
                    page++;
                    var commandSource = sourceConn.CreateCommand();
                    commandSource.CommandText = $"select [Key] from [{this.SourceSchema}].[{this.Table}] where 1=1 {this.SourceFilter} order by {orderByColumns} offset ({page}*{rowsPerPage}) rows fetch next {rowsPerPage} rows only";
                    using (var readerSource = await commandSource.ExecuteReaderAsync())
                    {
                        if (!readerSource.HasRows)
                        {
                            break;
                        }
                        while (readerSource.Read())
                        {
                            if (minKey == null)
                            {
                                minKey = readerSource.GetString(0);
                            }
                            maxKey = readerSource.GetString(0);
                        }
                    }
                }
                using (var sourceConn = new SqlConnection(this.SourceConnectionString))
                {
                    await sourceConn.OpenAsync();
                    page++;
                    var commandSource = sourceConn.CreateCommand();
                    commandSource.CommandText = $"select [Key],[EffectiveDateUtc],[LastUpdateUtc] from [{this.SourceSchema}].[{this.Table}] where 1=1 {this.SourceFilter} and [Key]>=@MinKey and [Key]<=@MaxKey";
                    commandSource.Parameters.Add(new SqlParameter("@MinKey", minKey));
                    commandSource.Parameters.Add(new SqlParameter("@MaxKey", maxKey));
                    using (var readerSource = await commandSource.ExecuteReaderAsync())
                    {
                        while (readerSource.Read())
                        {
                            var sourceAction = new TableColumnKeys();
                            sourceAction.IsInsert = true;
                            sourceAction.Key = readerSource.GetString(0);
                            sourceAction.EffectiveDateUtc = readerSource.GetDateTime(1);
                            sourceAction.LastUpdateUtc = readerSource.GetDateTime(2);
                            sourceActions.Add($"{sourceAction.Key}_{sourceAction.EffectiveDateUtc.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}", sourceAction);
                        }
                    }
                }
                using (var targetConn = new SqlConnection(this.TargetConnectionString))
                {
                    await targetConn.OpenAsync();
                    var commandTarget = targetConn.CreateCommand();
                    commandTarget.CommandText = $"select [Key],[EffectiveDateUtc],[LastUpdateUtc] from [{this.TargetSchema}].[{this.Table}] where [Key]>=@MinKey and [Key]<=@MaxKey";
                    commandTarget.Parameters.Add(new SqlParameter("@MinKey", minKey));
                    commandTarget.Parameters.Add(new SqlParameter("@MaxKey", maxKey));
                    using (var readerTarget = await commandTarget.ExecuteReaderAsync())
                    {
                        while (readerTarget.Read())
                        {
                            var targetAction = new TableColumnKeys();
                            targetAction.Key = readerTarget.GetString(0);
                            targetAction.EffectiveDateUtc = readerTarget.GetDateTime(1);
                            targetAction.LastUpdateUtc = readerTarget.GetDateTime(2);
                            var targetActionKey = $"{targetAction.Key}_{targetAction.EffectiveDateUtc.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}";
                            targetActions.Add(targetActionKey, targetAction);
                            if (sourceActions.ContainsKey(targetActionKey))
                            {
                                var sourceAction = sourceActions[targetActionKey];
                                sourceAction.IsInsert = false;
                                if (sourceAction.LastUpdateUtc < DateTime.UtcNow.AddMinutes(-30)
                                    && sourceAction.LastUpdateUtc > targetAction.LastUpdateUtc
                                    && (sourceAction.LastUpdateUtc - targetAction.LastUpdateUtc) > TimeSpan.FromMinutes(2))
                                {
                                    sourceAction.IsUpdate = true;
                                    //Console.WriteLine($"IsUpdate=true {this.Table} Key={sourceAction.Key}, EffectiveDateUtc={sourceAction.EffectiveDateUtc.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, SourceLastUpdateUtc={sourceAction.LastUpdateUtc.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}, TargetLastUpdateUtc={targetAction.LastUpdateUtc.ToString("yyyy-MM-dd HH:mm:ss.fffffff")}");
                                }
                            }
                            else
                            {
                                targetAction.IsDelete = true;
                            }
                        }
                    }
                }
                foreach (var (k, v) in targetActions)
                {
                    if (v.IsDelete)
                    {
                        using (var targetConn = new SqlConnection(this.TargetConnectionString))
                        {
                            await targetConn.OpenAsync();
                            var command = targetConn.CreateCommand();
                            command.CommandTimeout = 600;
                            command.CommandText = $"delete from [{this.TargetSchema}].[{this.Table}] where [Key]=@Key and [EffectiveDateUtc]=@EffectiveDateUtc";
                            command.Parameters.Add(new SqlParameter("@Key", v.Key));
                            command.Parameters.Add(new SqlParameter("@EffectiveDateUtc", SqlDbType.DateTime2) { Value = v.EffectiveDateUtc });
                            watch.Restart();
                            var deleteCount = await command.ExecuteNonQueryAsync();
                            watch.Stop();
                            deletes += deleteCount;
                            Console.WriteLine($"  {this.Table} deleted {deleteCount} rows in {watch.Elapsed} with {GetSqlKeys(command)}");
                        }
                    }
                }
                foreach (var (k, v) in sourceActions)
                {
                    if (rows % 1000 == 0)
                    {
                        Console.WriteLine($"{this.Table} rows: {rows} time: {DateTime.UtcNow.ToString("HH:mm:ss")}");
                    }
                    rows++;
                    if (v.IsInsert || v.IsUpdate)
                    {
                        await Task.Delay(this.WaitBetweenMergeCommands);
                        using (var sourceConn = new SqlConnection(this.SourceConnectionString))
                        {
                            await sourceConn.OpenAsync();
                            var command = sourceConn.CreateCommand();
                            command.CommandText = $"select * from [{this.SourceSchema}].[{this.Table}] where [Key]=@Key and [EffectiveDateUtc]=@EffectiveDateUtc";
                            command.Parameters.Add(new SqlParameter("@Key", v.Key));
                            command.Parameters.Add(new SqlParameter("@EffectiveDateUtc", SqlDbType.DateTime2) { Value = v.EffectiveDateUtc });
                            using (var reader = await command.ExecuteReaderAsync())
                            {
                                while (reader.Read())
                                {
                                    using (var targetConn = new SqlConnection(this.TargetConnectionString))
                                    {
                                        await targetConn.OpenAsync();
                                        var mergeCommand = targetConn.CreateCommand();
                                        mergeCommand.CommandTimeout = 300;
                                        AddParameters(reader, mergeCommand);
                                        if (v.IsInsert)
                                        {
                                            mergeCommand.CommandText = insertCmdText;
                                            try
                                            {
                                                watch.Restart();
                                                var insertCount = await mergeCommand.ExecuteNonQueryAsync();
                                                watch.Stop();
                                                inserts += insertCount;
                                                Console.WriteLine($"  {this.Table} inserted {insertCount} rows in {watch.Elapsed} with {GetSqlKeys(mergeCommand)}");
                                            }
                                            catch (Exception insertException)
                                            {
                                                errors += 1;
                                                Console.WriteLine($"  {this.Table} {GetSqlKeys(mergeCommand)} {insertException.GetType()}: {insertException.Message}");
                                            }
                                        }
                                        else if (v.IsUpdate)
                                        {
                                            mergeCommand.CommandText = updateCmdText;
                                            try
                                            {
                                                watch.Restart();
                                                var updateCount = await mergeCommand.ExecuteNonQueryAsync();
                                                watch.Stop();
                                                updates += updateCount;
                                                Console.WriteLine($"  {this.Table} updated {updateCount} rows in {watch.Elapsed} with {GetSqlKeys(mergeCommand)}");
                                            }
                                            catch (Exception updateException)
                                            {
                                                errors += 1;
                                                Console.WriteLine($"  {this.Table} {GetSqlKeys(mergeCommand)} {updateException.GetType()}: {updateException.Message}");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Console.WriteLine($"{this.Table} rows: {rows} inserts:{inserts}, updates:{updates}, deletes: {deletes} errors:{errors}");
        }
        public async Task CopyTableAsync()
        {
            await LoadSourceTableMetadataAsync();
            if (columnDataTypes.ContainsKey("Key") && columnDataTypes.ContainsKey("EffectiveDateUtc") && columnDataTypes.ContainsKey("LastUpdateUtc"))
            {
                await SmartCopyAsync();
            }
            else
            {
                await BruteForceCopyAsync();
            }
        }
        private string GetSqlKeys(DbCommand command)
        {
            var sb = new StringBuilder();
            foreach (DbParameter parameter in command.Parameters)
            {
                if (parameter.ParameterName == "@Key")
                {
                    sb.Append($" {parameter.ParameterName}='{parameter.Value}'");
                }
                else if (parameter.DbType == DbType.DateTime2)
                {
                    sb.Append($" {parameter.ParameterName}='{((DateTime)parameter.Value).ToString("yyyy-MM-dd HH:mm:ss.fffffff")}'");
                }
            }

            return sb.ToString();
        }

        private void AddParameters(SqlDataReader reader, SqlCommand command)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnDataType = columnDataTypes[reader.GetName(i)];
                var parameterName = $"@{reader.GetName(i)}";
                if ("@Key".Equals(parameterName))
                {
                    command.Parameters.Add(new SqlParameter(parameterName, DbType.String) { Value = reader.GetValue(i), Size = 255 });
                }
                else if ("datetime2".Equals(columnDataType, StringComparison.InvariantCultureIgnoreCase))
                {
                    command.Parameters.Add(new SqlParameter(parameterName, SqlDbType.DateTime2) { Value = reader.GetValue(i) });
                }
                else if ("bigint".Equals(columnDataType, StringComparison.InvariantCultureIgnoreCase))
                {
                    command.Parameters.Add(new SqlParameter(parameterName, SqlDbType.BigInt) { Value = reader.GetValue(i) });
                }
                else
                {
                    command.Parameters.AddWithValue(parameterName, reader.GetValue(i));
                }
            }
        }
    }
}