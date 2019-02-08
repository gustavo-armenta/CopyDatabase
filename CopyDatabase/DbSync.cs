using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace CopyDatabase
{
    class DbSync
    {
        public string SourceConnectionString { get; set; }
        public string SourceSchema { get; set; }
        public string SourceSchemaFilter { get; set; }
        public string SourceFilter { get; set; }
        public string TargetConnectionString { get; set; }
        public string TargetSchema { get; set; }
        public int Workers { get; set; }

        public async Task RunAsync()
        {
            var queue = new ConcurrentQueue<string>();
            using (var connection = new SqlConnection(this.SourceConnectionString))
            {
                await connection.OpenAsync();
                var command = connection.CreateCommand();
                command.CommandText = $"select name from sys.tables where name not in ('__pk_indexes','__MigrationHistory','__BuyMigrationHistory') and schema_id=(select schema_id from sys.schemas where name='{this.SourceSchema}') {this.SourceSchemaFilter} order by name";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var table = reader.GetString(0);
                        Console.WriteLine($"enqueue {table}");
                        queue.Enqueue(table);
                    }
                }

                var workers = new List<Task>();
                for (int i = 0; i < this.Workers; i++)
                {
                    workers.Add(RunWorkerAsync(queue));
                }

                Console.WriteLine("Waiting for workers to complete");
                Task.WaitAll(workers.ToArray());
            }
        }

        async Task RunWorkerAsync(ConcurrentQueue<string> queue)
        {
            while (!queue.IsEmpty)
            {
                if (queue.TryDequeue(out string table))
                {
                    try
                    {
                        var tableSync = new TableSync();
                        tableSync.SourceConnectionString = this.SourceConnectionString;
                        tableSync.SourceFilter = this.SourceFilter;
                        tableSync.SourceSchema = this.SourceSchema;
                        tableSync.TargetConnectionString = this.TargetConnectionString;
                        tableSync.TargetSchema = this.TargetSchema;
                        tableSync.WaitBetweenMergeCommands = TimeSpan.FromMilliseconds(100);
                        tableSync.Table = table;
                        await tableSync.CopyTableAsync();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{table} {ex}");
                    }
                }
            }
        }
    }
}
