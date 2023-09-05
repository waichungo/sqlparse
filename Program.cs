// See https://aka.ms/new-console-template for more information
using System.Diagnostics.Metrics;
using System.Text.RegularExpressions;

// Start();
ParseSQL();
void Start()
{
    using var file = File.OpenRead(@"H:\libgendb\libgen_compact.sql");
    using var stream = new StreamReader(file);
    var line = "";
    var maxSize = 1024 * 1024 * 10;
    var counter = 0;
    while ((line = stream.ReadLine()) != null)
    {
        counter += line.Length;
        File.AppendAllText("sql.sql", line + Environment.NewLine);
        if (line.Length > (1024 * 1024) || counter > maxSize)
        {
            break;
        }

    }
}
void ParseSQL()
{
    using var file = File.OpenRead(@"H:\libgendb\libgen_compact.sql");
    using var stream = new StreamReader(file);
    var line = "";

    var parsed = new Dictionary<string, Dictionary<string, object>>();
    var table = "";
    var rgx = new Regex(@"INSERT INTO `(?<table>\w)+`\s+\((?<keys>.*)+\)\s+VALUES \((?<values>.*)+\);");
    while ((line = stream.ReadLine()) != null)
    {
        //INSERT INTO `updated` () VALUES ()
        if (line.StartsWith("INSERT INTO"))
        {

            var tableStart = line.IndexOf('`');
            var tableEnd = line.IndexOf('`', tableStart + 1);

            var valuesName = "VALUES";
            var valuesIndex =line.IndexOf(valuesName);

            var keyStart= line.IndexOf('(');
            var keyEnd= line.IndexOf(')', valuesIndex -3);

            var valStart= line.IndexOf('(',valuesIndex);
            var valEnd= line.LastIndexOf(");");

            // var match3 = rgx.IsMatch(line);
            table=line.Substring(tableStart+1,(tableEnd-1)-tableStart);
            var keys=Regex.Split( line.Substring(keyStart+1,keyEnd-(keyStart+1)),",{1,}").Select(e=>e.Replace("`","")).ToList();
            var values=Regex.Split( line.Substring(valStart+1,valEnd-(valStart+1)),",{1,1}").Where(e=>e!="''").ToList();

            var entry = new Dictionary<string, object>();


        }
    }
}