// See https://aka.ms/new-console-template for more information
using System.Diagnostics.Metrics;
using System.Text.Json;
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

List<string> ParseSQLValues(string valueString)
{
    var values = new List<string>();
    // var values = new List<string>();
    var escapedQuotes = new Regex(@"\\+'", RegexOptions.Multiline).Matches(valueString).Select(e => e.Index + e.Value.Length).ToList();

    var startQuotes = new Regex(@"'", RegexOptions.Multiline).Matches(valueString).Select(e => e.Index).Where(e =>
        {            
            var prevIdx = e - 1;
            if (prevIdx >= 0)
            {
                if (valueString[prevIdx] == '\'' || valueString[prevIdx] == '\\')
                {
                    
                    return false;
                }
            }
            var nextIdx = e + 1;
            if (nextIdx < valueString.Length)
            {
                return valueString[nextIdx] != '\'';
            }
            return true;
        }).OrderBy(e => e).ToArray();
    if (escapedQuotes.Count > 0)
    {
        startQuotes = startQuotes.Where(e => !escapedQuotes.Contains(e)).ToArray();
    }
    var quotePairs = new Dictionary<int, int>(startQuotes.Length / 2);
    if (startQuotes.Length % 2 != 0)
    {
        Console.WriteLine("Not even");
        return values;
    }

    for (int i = 0; i < startQuotes.Length; i += 2)
    {
        quotePairs.Add(startQuotes[i], startQuotes[i + 1]);
    }
    var fieldStarts = new Regex(@",", RegexOptions.Multiline).Matches(valueString).Select(e => e.Index + 1).Where(e =>
    {
        var idx = e - 1;
        if (quotePairs.Any(pair => idx > pair.Key && idx < pair.Value))
        {
            return false;
        }
        var prevIdx = e - 2;
        if (prevIdx >= 0)
        {
            if (valueString[prevIdx] == ',')
            {
                return false;
            }
        }
        if (e < valueString.Length)
        {
            return valueString[e] != ',';
        }
        return true;
    }).OrderBy(e => e).ToArray();


    if (fieldStarts.Length > 0)
    {
        values.Add(valueString.Substring(0, fieldStarts[0] - 1));
    }
    if (fieldStarts.Length > 1)
    {
        for (int i = 1; i < fieldStarts.Length; i++)
        {
            values.Add(valueString.Substring(fieldStarts[i - 1], (fieldStarts[i] - 1) - fieldStarts[i - 1]));

        }
    }
    values.Add(valueString.Substring(fieldStarts[fieldStarts.Length - 1], valueString.Length - fieldStarts[fieldStarts.Length - 1]));
    
    return values;
}
void ParseSQL()
{
    using var file = File.OpenRead(@"H:\libgendb\libgen_compact.sql");
    using var stream = new StreamReader(file);
    var line = "";

    Bag parsed = new Bag();
    var table = "";
    var rgx = new Regex(@"\),\(\d+,");
    var lineCounter = 0;
    while ((line = stream.ReadLine()) != null)
    {
        //INSERT INTO `updated` () VALUES ()
        if (line.StartsWith("INSERT INTO"))
        {
            lineCounter++;
            var tableStart = line.IndexOf('`');
            var tableEnd = line.IndexOf('`', tableStart + 1);

            var valuesName = "VALUES";
            var valuesIndex = line.IndexOf(valuesName);

            var keyStart = line.IndexOf('(');
            var keyEnd = line.IndexOf(')', valuesIndex - 3);

            var valStart = line.IndexOf('(', valuesIndex);
            var valEnd = line.LastIndexOf(");");

            // var match3 = rgx.IsMatch(line);
            table = line.Substring(tableStart + 1, (tableEnd - 1) - tableStart);
            var keys = Regex.Split(line.Substring(keyStart + 1, keyEnd - (keyStart + 1)), ",{1,}").Select(e => e.Replace("`", "").Trim()).ToList();

            //),(1579,

            var valuesString = line.Substring(valStart + 1, valEnd - (valStart + 1));
            var valueCollection = rgx.Matches(valuesString).Select(e => e.Index + 3).OrderBy(e => e).ToArray();

            var valueEntries = new List<string>(valueCollection.Length);
            if (valueCollection.Length > 0)
            {
                valueEntries.Add(valuesString.Substring(0, valueCollection[0] - 3));
            }
            if (valueCollection.Length > 2)
            {

                var index = 0;
                while (index < (valueCollection.Length - 1))
                {
                    var row = valuesString.Substring(valueCollection[index], (valueCollection[index + 1] - 3) - valueCollection[index]);

                    valueEntries.Add(row);
                    index++;
                }
            }
            foreach (var row in valueEntries)
            {
                var values = ParseSQLValues(row);
                if (values.Count == keys.Count)
                {
                    var entry = new Dictionary<string, object>();
                    for (int i = 0; i < keys.Count; i++)
                    {
                        var key = keys[i];
                        object val = values[i];

                        if (long.TryParse(val as string, out var longValue))
                        {
                            val = longValue;
                        }
                        else
                        {
                            val = Regex.Replace(val as string, @"(^')|('$)", "");
                        }
                        if (val is string valstr)
                        {
                            if (valstr.Length == 0)
                            {
                                continue;
                            }
                        }
                        entry.Add(key, val);
                    }

                    parsed = new Bag{TableName= table,Data= entry};

                    var jsnline = JsonSerializer.Serialize(parsed, new JsonSerializerOptions
                    {
                        // WriteIndented = true
                    });
                    File.AppendAllText("parse.txt", jsnline + Environment.NewLine);


                }
                else
                {
                    Console.WriteLine("Key length did not match value length");
                }
            }


            Console.WriteLine($"Parsed {lineCounter} lines");

        }
    }
}
public class Bag{
    public string TableName{get;set;}="";
    public Dictionary<string,object> Data{get;set;}=new();
}