using Microsoft.Extensions.Configuration;
using System;
using System.IO;

namespace CopyDatabase
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddJsonFile("appsettings.Development.json", optional: true)
                .Build();
            var appsettings = config.Get<AppSettings>();
            
            var instance = new DbSync();
            instance.Workers = appsettings.Workers;
            instance.SourceConnectionString = appsettings.Source.ConnectionString;
            instance.SourceSchema = appsettings.Source.Schema;
            instance.SourceSchemaFilter = appsettings.Source.SchemaFilter;
            instance.SourceFilter = appsettings.Source.Filter;
            instance.TargetConnectionString = appsettings.Target.ConnectionString;
            instance.TargetSchema = appsettings.Target.Schema;

            try
            {
                instance.RunAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            //Console.WriteLine("Press ENTER to exit");
            //Console.ReadLine();
        }
    }
}
