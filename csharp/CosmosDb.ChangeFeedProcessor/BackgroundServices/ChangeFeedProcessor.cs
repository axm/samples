using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;

public class ChangeProcessorService : IHostedService
{
    private readonly Container _container;
    private readonly IChangeFeedProcessorHandler _changeFeedProcessorHandler;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly IOptionsSnapshot<ChangeProcessorOptions> _options;
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
        _options = options;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Starting change feed processor. Poll interval: {PollIntervalInSeconds}s.",
            _options.Value.PollIntervalInSeconds
        );

        _processor = _container
            .GetChangeFeedProcessorBuilder<object>("processorName", HandleChangesAsync)
            .WithInstanceName("hostName")
            .WithLeaseContainer(_container)
            .WithErrorNotification(ErrorDelegateAsync)
            .WithPollInterval(TimeSpan.FromSeconds(_options.Value.PollIntervalInSeconds))
            .Build();

        await _processor.StartAsync();
    }

    private async Task ErrorDelegateAsync(string leaseToken, Exception exception)
    {
        switch (exception)
        {
            case CosmosException cosmosException:
                if (cosmosException.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    _logger.LogWarning(cosmosException, "Too many requests for lease {LeaseToken}.", leaseToken);
                    return;
                }
                break;
        }

        _logger.LogError(exception, "Error occurred for lease {LeaseToken}. Stopping.", leaseToken);
        _applicationLifetime.StopApplication();
        await Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping change feed processor.");

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
