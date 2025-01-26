using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;

public class ChangeProcessorService : IHostedService
{
    private readonly Container _container;
    private readonly IChangeFeedProcessorHandler _changeFeedProcessorHandler;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly ILogger<ChangeProcessorService> _logger;
    private Microsoft.Azure.Cosmos.ChangeFeedProcessor? _processor;

    public ChangeProcessorService(
        CosmosClient cosmosClient,
        IChangeFeedProcessorHandler changeFeedProcessorHandler,
        IHostApplicationLifetime applicationLifetime,
        IOptionsSnapshot<ChangeProcessorOptions> options,
        ILogger<ChangeProcessorService> logger
     )
    {
        _container = cosmosClient.GetContainer(options.Value.DatabaseId, options.Value.ContainerId);
        _changeFeedProcessorHandler = changeFeedProcessorHandler;
        _applicationLifetime = applicationLifetime;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _processor = _container
            .GetChangeFeedProcessorBuilder<object>("processorName", HandleChangesAsync)
            .WithInstanceName("hostName")
            .WithLeaseContainer(_container)
            .WithErrorNotification(ErrorDelegateAsync)
            .Build();

        await _processor.StartAsync();
    }

    private async Task ErrorDelegateAsync(string leaseToken, Exception exception)
    {
        _logger.LogError(exception, "Error occurred for lease {LeaseToken}", leaseToken);

        _applicationLifetime.StopApplication();
        await Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_processor is not null)
        {
            await _processor.StopAsync();
        }
    }

    private async Task HandleChangesAsync(IReadOnlyCollection<object> changes, CancellationToken cancellationToken)
    {
        await _changeFeedProcessorHandler.HandleAsync(changes, cancellationToken);
    }
}
