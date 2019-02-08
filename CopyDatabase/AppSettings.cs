using System;

namespace CopyDatabase
{
    class AppSettings
    {
        public int Workers { get; set; }
        public Source Source { get; set; }
        public Target Target { get; set; }
    }

    class Source
    {
        public string ConnectionString { get; set; }
        public string Schema { get; set; }
        public string SchemaFilter { get; set; }
        public string Filter { get; set; }
    }

    class Target
    {
        public string ConnectionString { get; set; }
        public string Schema { get; set; }
    }
}
