using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;

public class DeadletterService : IDeadletterService
{
    private readonly CosmosClient _cosmosClient;
    private readonly IOptionsSnapshot<DeadletterOptions> _options;

    public DeadletterService(CosmosClient cosmosClient, IOptionsSnapshot<DeadletterOptions> options)
    {
        _cosmosClient = cosmosClient;
        _options = options;
    }

    public async Task DeadletterUnknownDocumentsAsync(List<object> documents, CancellationToken cancellationToken)
    {
        var deadletterContainer = _cosmosClient.GetContainer(_options.Value.DatabaseId, _options.Value.ContainerId);
    }
}