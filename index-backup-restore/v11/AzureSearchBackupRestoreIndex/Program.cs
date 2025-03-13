// This is a prototype tool that allows for extraction of data from a search index
// Since this tool is still under development, it should not be used for production usage

using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AzureSearchBackupRestore
{
    class Program
    {

        private static string SourceSearchServiceName;
        private static string SourceAdminKey;
        private static string SourceIndexName;
        private static string TargetSearchServiceName;
        private static string TargetAdminKey;
        private static string TargetIndexName;
        private static bool RecreateTargetIndex;
        private static string BackupDirectory;
        private static string MetadataDirectory;
        private static string FacetCategory;

        private static SearchIndexClient SourceIndexClient;
        private static SearchClient SourceSearchClient;
        private static SearchIndexClient TargetIndexClient;
        private static SearchClient TargetSearchClient;

        private static int MaxBatchSize = 500;                        // JSON files will contain this many documents / file and can be up to 1000
        private static int ParallelizedJobs = 10;                     // Output content in parallel jobs
        private static int MaxRecordsSkippablePerRequest = 100000;    // Fixed by Azure. Do not change

        static void Main(string[] args)
        {

            // 1. Get source, target index details and other configurations from appsettings.json file
            ConfigurationSetup();

            if (!Directory.Exists(BackupDirectory))
            {
                Directory.CreateDirectory(BackupDirectory);
            }

            if (!Directory.Exists(MetadataDirectory))
            {
                Directory.CreateDirectory(MetadataDirectory);
            }

            string finishedFilePath = Path.Combine(MetadataDirectory, "Finished.txt");

            if (!File.Exists(finishedFilePath))
            {
                using (File.Create(finishedFilePath)) { }
            }

            List<string> FinishedFacetContent = new List<string>();

            if (File.Exists(finishedFilePath))
            {
                FinishedFacetContent = File.ReadAllLines(finishedFilePath).ToList();
                FinishedFacetContent.Reverse();
            }

            // 2. Delete and create the Target index
            if (RecreateTargetIndex)
            {
                GetSourceIndexSchema();
                DeleteTargetIndex();
                CreateTargetIndex();
            }

            // 3. Get the distinct list of facets values from the Source index, skip finished indexes
            List<string> DistinctFacetContent = GetFacetValues(FacetCategory);
            Console.WriteLine($"\n Count of {FacetCategory}: {DistinctFacetContent.Count}");
            Console.WriteLine($"\n List of {FacetCategory}:\n {string.Join("\n ", DistinctFacetContent.ToList())}");

            DistinctFacetContent = DistinctFacetContent.Except(FinishedFacetContent).ToList();

            try
            {

                // 4. Migrate one facet at a time
                foreach (var _FacetContent in DistinctFacetContent)
                {
                    // 4.1 Backup from source index
                    Console.WriteLine("\nStarting Index Backup of " + _FacetContent);
                    BackupIndexAndDocuments(_FacetContent);

                    // 4.2 Restore to target index
                    ImportDocumentsFromJSON(_FacetContent);

                    // 4.3 Validate the contents is in target index
                    int sourceCount = GetCurrentDocCount(SourceSearchClient, FacetCategory, _FacetContent);
                    int targetCount = GetCurrentDocCount(TargetSearchClient, FacetCategory, _FacetContent);

                    Console.WriteLine(" Source index contains {0} docs", sourceCount);
                    Console.WriteLine(" Target index contains {0} docs", targetCount);

                    if (sourceCount == targetCount)
                    {
                        var Completed = new List<string>();
                        Completed.Insert(0, _FacetContent);

                        File.AppendAllLines(MetadataDirectory + "\\" + "Finished.txt", Completed);
                    }

                }

            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                Console.WriteLine("Error in execution");
            }

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        static void ConfigurationSetup()
        {

            IConfigurationBuilder builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");
            IConfigurationRoot configuration = builder.Build();

            SourceSearchServiceName = configuration["SourceSearchServiceName"];
            SourceAdminKey = configuration["SourceAdminKey"];
            SourceIndexName = configuration["SourceIndexName"];

            TargetSearchServiceName = configuration["TargetSearchServiceName"];
            TargetAdminKey = configuration["TargetAdminKey"];
            TargetIndexName = configuration["TargetIndexName"];

            RecreateTargetIndex = bool.Parse(configuration["RecreateTargetIndex"]);

            FacetCategory = configuration["FacetCategory"];
            BackupDirectory = configuration["BackupDirectory"];
            MetadataDirectory = configuration["MetadataDirectory"];

            SourceIndexClient = new SearchIndexClient(new Uri("https://" + SourceSearchServiceName + ".search.windows.net"), new AzureKeyCredential(SourceAdminKey));
            SourceSearchClient = SourceIndexClient.GetSearchClient(SourceIndexName);

            TargetIndexClient = new SearchIndexClient(new Uri($"https://" + TargetSearchServiceName + ".search.windows.net"), new AzureKeyCredential(TargetAdminKey));
            TargetSearchClient = TargetIndexClient.GetSearchClient(TargetIndexName);

            Console.WriteLine("Configuration:");
            Console.WriteLine("\n  Source service and index {0}, {1}", SourceSearchServiceName, SourceIndexName);
            Console.WriteLine("\n  Target service and index: {0}, {1}", TargetSearchServiceName, TargetIndexName);
            Console.WriteLine("\n  Backup directory: " + BackupDirectory);
            Console.WriteLine("\n  Metadata directory: " + MetadataDirectory);
        }

        static void GetSourceIndexSchema()
        {
            // Backup the index schema to the specified backup directory
            Console.WriteLine("\n Backing up source index schema to {0}\r\n", BackupDirectory + "\\" + SourceIndexName + ".schema");
            File.WriteAllText(BackupDirectory + "\\" + SourceIndexName + ".schema", GetIndexSchema());
        }

        static void BackupIndexAndDocuments(string FacetValue)
        {

            int indexDocCount = GetDocCountForFacetAsync(SourceSearchClient, FacetCategory, FacetValue);

            if (indexDocCount > MaxRecordsSkippablePerRequest)
                Console.WriteLine($"ERROR: Files count: {indexDocCount} for index: {FacetValue}");
                
            string content = $"{FacetValue}: {indexDocCount}\n";
            Console.WriteLine($"Files count: {indexDocCount} for index: {FacetValue}");

            WriteIndexDocuments(indexDocCount, FacetCategory, FacetValue);

        }

        private static int GetDocCountForFacetAsync(SearchClient searchClient, string facetField, string facetValue)
        {
            try
            {
                string filterQuery = $"{facetField} eq '{facetValue}'";

                SearchOptions options = new SearchOptions
                {
                    Filter = filterQuery,
                    Size = 0, // We only need the count, not the actual documents
                    IncludeTotalCount = true
                };

                SearchResults<Dictionary<string, object>> response = searchClient.Search<Dictionary<string, object>>("*", options);
                return Convert.ToInt32(response.TotalCount);
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error: {0}", ex.Message.ToString());
            }

            return -1;
        }

        static void WriteIndexDocuments(int CurrentDocCount, string facetField, string facetValue)
        {
            string backupFolder = BackupDirectory + "\\" + facetValue;
            bool exists = Directory.Exists(backupFolder);

            if (!exists)
                Directory.CreateDirectory(backupFolder);
            else
                foreach(var file in Directory.EnumerateFiles(backupFolder))
                {
                    File.Delete(file);
                }

            // Write document files in batches (per MaxBatchSize) in parallel
            string IDFieldName = GetIDFieldName();
            int FileCounter = 0;
            for (int batch = 0; batch <= (CurrentDocCount / MaxBatchSize); batch += ParallelizedJobs)
            {

                List<Task> tasks = new List<Task>();
                for (int job = 0; job < ParallelizedJobs; job++)
                {
                    FileCounter++;
                    int fileCounter = FileCounter;
                    if ((fileCounter - 1) * MaxBatchSize < CurrentDocCount)
                    {
                        Console.WriteLine("  Backing up source documents to {0} - (batch max. size = {1})", backupFolder + "\\" + SourceIndexName + "-" + facetValue + fileCounter + ".json", MaxBatchSize);

                        tasks.Add(Task.Factory.StartNew(() =>
                            ExportToJSON((fileCounter - 1) * MaxBatchSize, facetField, facetValue, backupFolder + "\\" + SourceIndexName + "-" + facetValue + fileCounter + ".json")
                        ));
                    }

                }
                Task.WaitAll(tasks.ToArray());  // Wait for all the stored procs in the group to complete
            }

            return;
        }

        static void ExportToJSON(int skip, string facetField, string facetValue, string FileName)
        {
            // Extract all the documents from the selected index to JSON files in batches of 500 docs / file
            string json = string.Empty;
            try
            {
                string filterQuery = $"{facetField} eq '{facetValue}'";
                SearchOptions options = new SearchOptions()
                {
                    SearchMode = SearchMode.All,
                    Size = MaxBatchSize,
                    Filter = filterQuery,
                    Skip = skip
                };
                options.Select.Add("id");
                options.Select.Add("content");
                options.Select.Add("embedding");
                options.Select.Add("category");
                options.Select.Add("source_url");
                options.Select.Add("file_id");

                SearchResults<SearchDocument> response = SourceSearchClient.Search<SearchDocument>("*", options);

                foreach (var doc in response.GetResults())
                {
                    json += JsonSerializer.Serialize(doc.Document) + ",";
                    json = json.Replace("\"Latitude\":", "\"type\": \"Point\", \"coordinates\": [");
                    json = json.Replace("\"Longitude\":", "");
                    json = json.Replace(",\"IsEmpty\":false,\"Z\":null,\"M\":null,\"CoordinateSystem\":{\"EpsgId\":4326,\"Id\":\"4326\",\"Name\":\"WGS84\"}", "]");
                    json += "\r\n";
                }

                // Output the formatted content to a file
                json = json.Substring(0, json.Length - 3); // remove trailing comma
                File.WriteAllText(FileName, "{\"value\": [");
                File.AppendAllText(FileName, json);
                File.AppendAllText(FileName, "]}");
                Console.WriteLine("  Total documents: {0}", response.GetResults().Count().ToString());
                json = string.Empty;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }
        }

        private static List<string> GetFacetValues(string facetField)
        {
            List<string> facetValues = new List<string>();
            SearchOptions options = new SearchOptions
            {
                SearchMode = SearchMode.All,
                Facets = {$"{facetField}, count:5000"},
                Select = {facetField}
            };

            SearchResults<Dictionary<string, object>> results = SourceSearchClient.Search<Dictionary<string, object>>("*", options);

            foreach (FacetResult facetResult in results.Facets[facetField])
                facetValues.Add(facetResult.Value.ToString());

            facetValues = facetValues.Distinct().ToList<string>();
            facetValues.Sort();

            return facetValues;
        }

        static string GetIDFieldName()
        {
            // Find the id field of this index
            string IDFieldName = string.Empty;
            try
            {
                var schema = SourceIndexClient.GetIndex(SourceIndexName);
                foreach (var field in schema.Value.Fields)
                {
                    if (field.IsKey == true)
                    {
                        IDFieldName = Convert.ToString(field.Name);
                        break;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

            return IDFieldName;
        }

        static string GetIndexSchema()
        {

            // Extract the schema for this index
            // We use REST here because we can take the response as-is

            Uri ServiceUri = new Uri("https://" + SourceSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", SourceAdminKey);

            string Schema = string.Empty;
            try
            {
                Uri uri = new Uri(ServiceUri, "/indexes/" + SourceIndexName);
                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Get, uri);
                AzureSearchHelper.EnsureSuccessfulSearchResponse(response);
                Schema = response.Content.ReadAsStringAsync().Result.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

            string updatedSchema = ModifyIndexFields(Schema);

            return updatedSchema;
        }

        static string ModifyIndexFields(string schema)
        {
            // Path to the JSON file containing the new fields
            string newIndexFieldsFile = "./newindexfields.json";

            // Read the JSON file containing the new fields
            string newIndexFields = File.ReadAllText(newIndexFieldsFile);

            // Parse the JSON strings
            using (JsonDocument fieldsDocument = JsonDocument.Parse(newIndexFields))
            using (JsonDocument schemaDocument = JsonDocument.Parse(schema))
            {
                JsonElement fieldsRoot = fieldsDocument.RootElement;
                JsonElement schemaRoot = schemaDocument.RootElement;

                // Create a new JSON object with the updated fields
                using (var stream = new MemoryStream())
                {
                    using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
                    {
                        writer.WriteStartObject();

                        foreach (JsonProperty property in schemaRoot.EnumerateObject())
                        {
                            if (property.Name == "fields")
                            {
                                // Replace the fields property with the new fields
                                writer.WritePropertyName("fields");
                                fieldsRoot.GetProperty("fields").WriteTo(writer);
                            }
                            else
                            {
                                property.WriteTo(writer);
                            }
                        }

                        writer.WriteEndObject();
                    }

                    string updatedSchema = System.Text.Encoding.UTF8.GetString(stream.ToArray());
                    return updatedSchema;
                }
            }
        }
           
        private static bool DeleteTargetIndex()
        {
            Console.WriteLine("\n  Delete target index {0} in {1} search service, if it exists", TargetIndexName, TargetSearchServiceName);
            // Delete the index if it exists
            try
            {
                TargetIndexClient.DeleteIndex(TargetIndexName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error deleting index: {0}\r\n", ex.Message);
                Console.WriteLine("  Did you remember to set your SearchServiceName and SearchServiceApiKey?\r\n");
                return false;
            }

            return true;
        }

        static void CreateTargetIndex()
        {
            Console.WriteLine("\n  Create target index {0} in {1} search service", TargetIndexName, TargetSearchServiceName);
            // Use the schema file to create a copy of this index
            // I like using REST here since I can just take the response as-is

            string json = File.ReadAllText(BackupDirectory + "\\" + SourceIndexName + ".schema");

            // Do some cleaning of this file to change index name, etc
            json = "{" + json.Substring(json.IndexOf("\"name\""));
            int indexOfIndexName = json.IndexOf("\"", json.IndexOf("name\"") + 5) + 1;
            int indexOfEndOfIndexName = json.IndexOf("\"", indexOfIndexName);
            json = json.Substring(0, indexOfIndexName) + TargetIndexName + json.Substring(indexOfEndOfIndexName);

            Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", TargetAdminKey);

            try
            {
                Uri uri = new Uri(ServiceUri, "/indexes");
                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, uri, json);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error: {0}", ex.Message.ToString());
            }
        }

        static int GetCurrentDocCount(SearchClient searchClient, string facetField, string facetValue)
        {
            // Get the current doc count of the specified index
            try
            {

                string filterQuery = $"{facetField} eq '{facetValue}'";

                SearchOptions options = new SearchOptions()
                {
                    SearchMode = SearchMode.All,
                    Filter = filterQuery,
                    IncludeTotalCount = true
                };

                SearchResults<Dictionary<string, object>> response = searchClient.Search<Dictionary<string, object>>("*", options);
                return Convert.ToInt32(response.TotalCount);
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error: {0}", ex.Message.ToString());
            }

            return -1;
        }

        static void ImportDocumentsFromJSON(string FacetValue)
        {
            Console.WriteLine("\n  Upload index documents from saved JSON files");
            // Take JSON file and import this as-is to target index
            Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", TargetAdminKey);

            try
            {
                int _counter = 0;

                foreach (string fileName in Directory.GetFiles(BackupDirectory + "\\" + FacetValue, SourceIndexName + "*.json"))
                {
                    Console.WriteLine("  -Uploading documents from file {0}", fileName);
                    string json = File.ReadAllText(fileName);
                    Uri uri = new Uri(ServiceUri, "/indexes/" + TargetIndexName + "/docs/index");
                    HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, uri, json);
                    response.EnsureSuccessStatusCode();
                    
                    Thread.Sleep(10 * 1000);

                    if (_counter == 100)
                    {
                        Thread.Sleep(300 * 1000);
                        _counter = 0;
                    }

                    _counter++;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error: {0}", ex.Message.ToString());
            }
        }
    }
}