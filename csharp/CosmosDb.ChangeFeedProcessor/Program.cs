using Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;
using Azure.Messaging.EventHubs.Producer;

var builder = WebApplication.CreateBuilder(args);


builder.Services.AddHostedService<ChangeProcessorService>();
builder.Services.AddSingleton(sp => new EventHubProducerClient(
   connectionString: "your-connection-string",
   eventHubName: "your-hub-name"
));

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();
