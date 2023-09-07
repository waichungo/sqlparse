// See https://aka.ms/new-console-template for more information
using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Text.RegularExpressions;

Start();
// ParseSQL();
// ParseParsedJson();
void Start()
{
    var file = @"H:\libgendb\libgen.sql";
    TrimSql(file);
}
static string FormatBytes(ulong byteCount)
{
    string[] suf = { "B", "KB", "MB", "GB", "TB", "PB", "EB" }; //Longs run out around EB
    if (byteCount == 0)
        return "0 " + suf[0];
    var bytes = byteCount;
    int place = Convert.ToInt32(Math.Floor(Math.Log(bytes, 1024)));
    double num = Math.Round(bytes / Math.Pow(1024, place), 1);
    return num.ToString() + " " + suf[place];
}
void ParseParsedJson()
{
    var parseFile = @"H:\libgendb\bin\Release\net7.0\parse.txt";
    using var stream = File.OpenRead(parseFile);
    using var streamReader = new StreamReader(stream);
    var line = "";
    var entry = new Bag2();
    var lineCounter = 0;
    ulong readBytes = 0;
    while ((line = streamReader.ReadLine()) != null)
    {
        lineCounter++;
        readBytes = readBytes + (ulong)line.Length;
        try
        {
            entry = JsonSerializer.Deserialize<Bag2>(line);


            if (entry.Data.Language.ToLower().StartsWith("eng") && entry.Data.IdentifierWODash.Contains("9780698407367"))
            {
                Console.WriteLine(entry.Data);

            }


            if ((lineCounter % 20000) == 0)
            {
                Console.WriteLine($"Read {lineCounter} lines");
                Console.WriteLine($"Read {FormatBytes(readBytes)}\n");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }


    }
    Console.WriteLine($"Read {lineCounter} lines");
    Console.WriteLine($"Read {FormatBytes(readBytes)}");

}
void TrimSql(string path)
{
    //    var path=@"H:\libgendb\libgen_compact.sql";
    // var path = @"H:\libgendb\fiction.sql";
    using var file = File.OpenRead(path);
    using var stream = new StreamReader(file);
    var line = "";
    var maxSize = 1024 * 1024 * 10;
    var counter = 0;
    bool canRecordInsert = false;
    bool canRecordCreate = false;

    var start = DateTime.Now;

    var trimFile = Path.Combine(Path.GetDirectoryName(path), Path.GetFileNameWithoutExtension(path) + "_parsed" + Path.GetExtension(path));
    while ((line = stream.ReadLine()) != null)
    {
        counter += line.Length;

        if (canRecordInsert && line.StartsWith("INSERT"))
        {

            File.AppendAllText(trimFile, line + Environment.NewLine);
            canRecordInsert = false;
        }
        else
        {

            if (line.StartsWith("CREATE"))
            {
                canRecordCreate = true;
                canRecordInsert = true;

            }
            if (canRecordCreate)
            {
                if (line.Length > 5)
                {
                    File.AppendAllText(trimFile, line + Environment.NewLine);
                }
                canRecordCreate = false;

            }
        }

        if (line.Length > (1024 * 1024) || counter > maxSize)
        {
            break;
        }
        if (DateTime.Now.Subtract(start).TotalSeconds > 5)
        {

            start = DateTime.Now;
            var progress = (int)((double)(file.Position / file.Length) * 100);
            Console.WriteLine($"Progress is {progress}%");
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
Bag ParseInsert(string line)
{
    var parsed = new Bag();
    var rgx = new Regex(@"\),\(\d+,");
    var table = "";
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

            parsed = new Bag { TableName = table, Data = entry };

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
    return parsed;
}
void ParseSQLFile(string sqlFile)
{
    // using var file = File.OpenRead(@"H:\libgendb\libgen_compact.sql");
    using var file = File.OpenRead(sqlFile);
    using var stream = new StreamReader(file);
    var line = "";

    Bag parsed = new Bag();


    var lineCounter = 0;
    while ((line = stream.ReadLine()) != null)
    {
        //INSERT INTO `updated` () VALUES ()
        if (line.StartsWith("INSERT INTO"))
        {
            lineCounter++;
            parsed = ParseInsert(line);

            var progress = (int)((double)(file.Position / file.Length) * 100);
            Console.WriteLine($"Parsed {lineCounter} lines");
            Console.WriteLine($"Progress is {progress}%");

        }
    }
}
public class Bag
{
    public string TableName { get; set; } = "";
    public Dictionary<string, object> Data { get; set; } = new();
}

public class Bag2
{
    public string TableName { get; set; } = "";
    public BookInfo Data { get; set; } = new();
}

public class BookInfo
{
    // public string Title { get; set;}="";
    // public int No { get; set;}=0;
    public int ID { get; set; } = 0;
    public string Title { get; set; } = "";//   `Title` varchar(2000) DEFAULT '',
    public string VolumeInfo { get; set; } = "";//   `VolumeInfo` varchar(100) DEFAULT '',
    public string Series { get; set; } = "";//   `Series` varchar(300) DEFAULT '',
    public string Periodical { get; set; } = "";//   `Periodical` varchar(200) DEFAULT '',
    public string Author { get; set; } = "";//   `Author` varchar(1000) DEFAULT '',
    public string Year { get; set; } = "";//   `Year` varchar(14) DEFAULT '',
    public string Edition { get; set; } = "";//   `Edition` varchar(60) DEFAULT '',
    public string Publisher { get; set; } = "";//   `Publisher` varchar(400) DEFAULT '',
    public string City { get; set; } = "";//   `City` varchar(100) DEFAULT '',
    public string Pages { get; set; } = "";//   `Pages` varchar(100) DEFAULT '',
    public int PagesInFile { get; set; } = 0;//   `PagesInFile` int(10) unsigned NOT NULL DEFAULT 0,
    public string Language { get; set; } = "";//   `Language` varchar(150) DEFAULT '',
    public string Topic { get; set; } = "";//   `Topic` varchar(500) DEFAULT '',
    public string Library { get; set; } = "";//   `Library` varchar(50) DEFAULT '',
    public string Issue { get; set; } = "";//   `Issue` varchar(100) DEFAULT '',
    public string Identifier { get; set; } = "";//   `Identifier` varchar(300) DEFAULT '',
    public string ISSN { get; set; } = "";//   `ISSN` varchar(9) DEFAULT '',
    public string ASIN { get; set; } = "";//   `ASIN` varchar(200) DEFAULT '',
    public string UDC { get; set; } = "";//   `UDC` varchar(200) DEFAULT '',
    public string LBC { get; set; } = "";//   `LBC` varchar(200) DEFAULT '',
    public string DDC { get; set; } = "";//   `DDC` varchar(45) DEFAULT '',
    public string LCC { get; set; } = "";//   `LCC` varchar(45) DEFAULT '',
    public string Doi { get; set; } = "";//   `Doi` varchar(45) DEFAULT '',
    public string Googlebookid { get; set; } = "";//   `Googlebookid` varchar(45) DEFAULT '',
    public string OpenLibraryID { get; set; } = "";//   `OpenLibraryID` varchar(200) DEFAULT '',
    public string Commentary { get; set; } = "";//   `Commentary` varchar(10000) DEFAULT '',
    public int DPI { get; set; } = 0;//   `DPI` int(6) unsigned DEFAULT 0,
    public string Color { get; set; } = "";//   `Color` varchar(1) DEFAULT '',
    public string Cleaned { get; set; } = "";//   `Cleaned` varchar(1) DEFAULT '',
    public string Orientation { get; set; } = "";//   `Orientation` varchar(1) DEFAULT '',
    public string Paginated { get; set; } = "";//   `Paginated` varchar(1) DEFAULT '',
    public string Scanned { get; set; } = "";//   `Scanned` varchar(1) DEFAULT '',
    public string Bookmarked { get; set; } = "";//   `Bookmarked` varchar(1) DEFAULT '',
    public string Searchable { get; set; } = "";//   `Searchable` varchar(1) DEFAULT '',
    public long Filesize { get; set; } = 0;//   `Filesize` bigint(20) unsigned NOT NULL DEFAULT 0,
    public string Extension { get; set; } = "";//   `Extension` varchar(50) DEFAULT '',
    public string MD5 { get; set; } = "";//   `MD5` char(32) DEFAULT '',
    public string Generic { get; set; } = "";//   `Generic` char(32) DEFAULT '',
    public string Visible { get; set; } = "";//   `Visible` char(3) DEFAULT '',
    public string Locator { get; set; } = "";//   `Locator` varchar(733) DEFAULT '',
    public int Local { get; set; } = 0;//   `Local` int(10) unsigned DEFAULT 0,
    public string TimeAdded { get; set; } = "";//   `TimeAdded` timestamp NOT NULL DEFAULT '2000-01-01 05:00:00',
    public string TimeLastModified { get; set; } = "";//   `TimeLastModified` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
    public string Coverurl { get; set; } = "";//   `Coverurl` varchar(200) DEFAULT '',
    public string Tags { get; set; } = "";//   `Tags` varchar(500) DEFAULT '',
    public string IdentifierWODash { get; set; } = "";//   `IdentifierWODash` varchar(300) DEFAULT '',

}
