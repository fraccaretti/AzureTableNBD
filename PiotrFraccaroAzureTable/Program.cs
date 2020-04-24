using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bogus;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;

namespace PiotrFraccaroAzureTable
{
    public class CarEntity : TableEntity
    {
        public string City { get; set; }
        public string Brand { get; set; }
        public string Model { get; set; }
        public int ProductionYear { get; set; }
        public string BodyType { get; set; }
        public int SeatsCount { get; set; }
        public double EngineCapacity { get; set; }
        public string EngineType { get; set; }
        public int Mileage { get; set; }

        public CarEntity()
        {
        }
        
        public CarEntity(string partitionKey, string rowKey, string city, string brand, string model, int productionYear, string bodyType, int seatsCount, double engineCapacity, string engineType, int mileage)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            City = city;
            Brand = brand;
            Model = model;
            ProductionYear = productionYear;
            BodyType = bodyType;
            SeatsCount = seatsCount;
            EngineCapacity = engineCapacity;
            EngineType = engineType;
            Mileage = mileage;
        }

        public override string ToString()
        {
            return $"PartitionKey: {PartitionKey}, RowKey: {RowKey}, City: {City}, Brand: {Brand}, Model: {Model}, ProductionYear: {ProductionYear}, BodyType: {BodyType}, EngineCapacity: {EngineCapacity}, SeatsCount: {SeatsCount}, EngineType: {EngineType}, Mileage: {Mileage}";
        }
    }

    static class Program
    {
        public static CloudTable Table;
        private static async Task ShowMenu()
        {
            Console.WriteLine();
            Console.WriteLine("Wpisz odpowiedni symbol aby wykonać operacje:");
            Console.WriteLine("C - przygotuj bazę danych i wypełnij ją danymi");
            Console.WriteLine("Z1 - wykonaj zapytanie nr 1");
            Console.WriteLine("Z2 - wykonaj zapytanie nr 2");
            Console.WriteLine("Z3 - wykonaj zapytanie nr 3");
            Console.WriteLine("D - wyczysc baze danych");
            Console.WriteLine("X - wyjdz z programu");
            Console.WriteLine();
            await TakeAction();
        }

        private static async Task TakeAction()
        {
            var input = Console.ReadLine();
            switch (input)
            {
                case "c":
                case "C":
                    await DatabaseMockGenerator.InsertMockedCar(Table, 1000);
                    break;
                case "Z1":
                case "z1":
                    Console.WriteLine("Pokaz hatchback z 5 siedzeniami pochodzące z Afryki");
                    await SelectFiveSeatsHatchbackFromAfrica(Table);
                    break;
                case "Z2":
                case "z2":
                    Console.WriteLine("Pokaz samochody elektryczne z Azji");
                    await SelectAsianElectric(Table);
                    break;
                case "Z3": 
                case "z3":
                    Console.WriteLine("Pokaz sedany");
                    await SelectSedan(Table);
                    break;
                case "D":
                case "d":
                    await DeleteBatchData(Table);
                    break;
                case "X":
                case "x":
                    return;
            }
            await ShowMenu();
        }
        
        static async Task Main(string[] args)
        {
            Console.WriteLine("Witaj w naszym komisie!");
            var storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol");//connection string here
            var client = storageAccount.CreateCloudTableClient();
            Table = client.GetTableReference("Cars");
            await Table.CreateIfNotExistsAsync();
            await ShowMenu();
        }
        
        private static async Task SelectSedan(CloudTable table)
        {
            Console.WriteLine("Wyszukiwanie danych spelniajacych wybrane kryteria...");
            Console.WriteLine("-----------------------------------------------------");

            var sedanCondition = TableQuery.GenerateFilterCondition("BodyType", QueryComparisons.Equal, "Sedan");
            
            var query = new TableQuery<CarEntity>()
                .Where(sedanCondition);

            await table.ExecuteAsync(query, async segment =>
            {
                foreach (var car in segment.Results)
                {
                    await Console.Out.WriteLineAsync(car.ToString());
                }
            });

            Console.WriteLine("Dane wyswietlone poprawnie!");
        }

        private static async Task SelectAsianElectric(CloudTable table)
        {
            Console.WriteLine("Wyszukiwanie danych spelniajacych wybrane kryteria...");
            Console.WriteLine("-----------------------------------------------------");

            var asianCondition = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Asia");
            var electricCondition = TableQuery.GenerateFilterCondition("EngineType", QueryComparisons.Equal, "Electric");
            var combinedCondition = TableQuery.CombineFilters(asianCondition, TableOperators.And, electricCondition);
            var query = new TableQuery<CarEntity>()
                .Where(combinedCondition);

            await table.ExecuteAsync(query, async segment =>
            {
                foreach (var car in segment.Results)
                {
                    await Console.Out.WriteLineAsync(car.ToString());
                }
            });

            Console.WriteLine("Dane wyswietlone poprawnie!");
        }

        private static async Task SelectFiveSeatsHatchbackFromAfrica(CloudTable table)
        {
            Console.WriteLine("Wyszukiwanie danych spelniajacych wybrane kryteria...");
            Console.WriteLine("-----------------------------------------------------");

            var fiveSeatsCondition = TableQuery.GenerateFilterConditionForInt("SeatsCount", QueryComparisons.Equal, 5);
            var hatchbackCondition = TableQuery.GenerateFilterCondition("BodyType", QueryComparisons.Equal, "Hatchback");
            var africaCondition = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Africa");

            var combinedFiveSeatsAndHatchbackCondition = TableQuery.CombineFilters(fiveSeatsCondition, TableOperators.And, hatchbackCondition);
            var combinedCondition = TableQuery.CombineFilters(combinedFiveSeatsAndHatchbackCondition, TableOperators.And, africaCondition);
            
            var query = new TableQuery<CarEntity>()
                .Where(combinedCondition);

            await table.ExecuteAsync(query, async segment =>
            {
                foreach (var car in segment.Results)
                {
                    await Console.Out.WriteLineAsync(car.ToString());
                }
            });

            Console.WriteLine("Dane wyswietlone poprawnie!");
        }

        private static async Task DeleteBatchData(CloudTable table)
        {
            Console.WriteLine("Usuwanie danych...");

            int segmentNumber = 1;

            await ExecuteAsync(table, new TableQuery<CarEntity>(), async segment =>
            {
                Console.WriteLine(" - segment {0}", segmentNumber++);

                foreach (var carGroup in segment.GroupBy(x => x.PartitionKey))
                {
                    var batchOperation = new TableBatchOperation();

                    foreach (var car in carGroup)
                    {
                        batchOperation.Delete(car);
                    }

                    await table.ExecuteBatchAsync(batchOperation);
                }
            });

            Console.WriteLine("Baza usunieta!");
        }
        
        private static async Task ExecuteAsync<T>(this CloudTable table,
            TableQuery<T> query,
            Func<TableQuerySegment<T>, Task> onProgress = null,
            CancellationToken cancellationToken = default)
            where T : ITableEntity, new()
        {
            TableContinuationToken token = null;

            do
            {
                var segment = await table.ExecuteQuerySegmentedAsync(query, token);

                token = segment.ContinuationToken;

                if (onProgress != null)
                {
                    await onProgress.Invoke(segment);
                }
            } while (token != null && !cancellationToken.IsCancellationRequested);
        }
    }

    class DatabaseMockGenerator
    {
        public static async Task InsertMockedCar(CloudTable table, int count)
        {
            Console.WriteLine("Trwa generowanie danych, prosze czekac");
            for (int i = 0; i < count; i++)
            {
                var continents = new[] {"Asia", "Africa", "North America", "South America", "Europe", "Australia"};
                var testCars = new Faker<CarEntity>()
                    .CustomInstantiator(faker => new CarEntity(
                        continents[new Random().Next(0, continents.Length)],
                        i.ToString(),
                        faker.Address.City(),
                        faker.Vehicle.Manufacturer(),
                        faker.Vehicle.Model(),
                        faker.Random.Number(2000, 202),
                        faker.Vehicle.Type(),
                        faker.Random.Number(2, 6) == 3 ? 4 : faker.Random.Number(4, 6),
                        Math.Round(faker.Random.Double(1, 4), 1),
                        faker.Vehicle.Fuel(),
                        faker.Random.Int(100, 450000)
                    ));

                var car = testCars.Generate();
                var insertOp = TableOperation.InsertOrReplace(car);
                await table.ExecuteAsync(insertOp);
            }
            Console.WriteLine("Baza skonfigurowana!");
        }
    }
}