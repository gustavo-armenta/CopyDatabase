using System;

namespace CopyDatabase
{
    class TableColumnKeys
    {
        public bool IsInsert { get; set; }
        public bool IsUpdate { get; set; }
        public bool IsDelete { get; set; }
        public string Key { get; set; }
        public DateTime EffectiveDateUtc { get; set; }
        public DateTime LastUpdateUtc { get; set; }        
    }
}
