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
    var rgx = new Regex(@"\),\(\d+,");
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

//),(1579,

            var valuesString=line.Substring(valStart+1,valEnd-(valStart+1));
            var valueCollection=rgx.Matches(valuesString).Select(e=>e.Index+3).OrderBy(e=>e).ToArray();

            var values=new List<string>(valueCollection.Length);
            if(valueCollection.Length>0){
                values.Add(valuesString.Substring(0,valueCollection[0]-3));
            }
              if(valueCollection.Length>2){

                var index=0;
                while(index<(valueCollection.Length-1)){
                    var row=valuesString.Substring(valueCollection[index],(valueCollection[index+1]-3)- valueCollection[index]);
                    File.AppendAllText("parse.txt",row);
                    values.Add(row);                   
                    index++;
                }
              }

            var entry = new Dictionary<string, object>();


        }
    }
}