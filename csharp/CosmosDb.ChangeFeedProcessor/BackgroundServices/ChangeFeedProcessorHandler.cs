using System.Text.Json;
using Axm.CosmosDb.ChangeFeedProcessor.Documents;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using OpenTelemetry.Trace;

namespace Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;

public class ChangeFeedProcessorHandler : IChangeFeedProcessorHandler
{
    private readonly Tracer _tracer;
    private readonly EventHubProducerClient _eventHubProducerClient;
    private readonly IDeadletterService _deadletterService;
    private readonly ILogger<ChangeFeedProcessorHandler> _logger;

    public ChangeFeedProcessorHandler(
        TracerProvider tracerProvider,
        EventHubProducerClient eventHubProducerClient,
        IDeadletterService deadletterService,
        ILogger<ChangeFeedProcessorHandler> logger
    )
    {
        _tracer = tracerProvider.GetTracer("ChangeFeedProcessorHandler");
        _eventHubProducerClient = eventHubProducerClient;
        _logger = logger;
        _deadletterService = deadletterService;
    }

    public async Task HandleAsync(IReadOnlyCollection<object> changes, CancellationToken cancellationToken)
    {
        using TelemetrySpan span = _tracer.StartActiveSpan("HandleChanges");
        span.SetAttribute("change.count", changes.Count);

        var failureCount = 0;
        var document1List = new List<Document1>();
        var document2List = new List<Document2>();
        var unknownDocumentsList = new List<object>();

        foreach (var change in changes)
        {
            try
            {
                switch (change)
                {
                    case Document1 document1:
                        document1List.Add(document1);
                        break;
                    case Document2 document2:
                        document2List.Add(document2);
                        break;
                    default:
                        unknownDocumentsList.Add(change);
                        _logger.LogWarning("Unsupported change type: {ChangeType}", change.GetType().Name);
                        failureCount++;
                        break;
                }
            }
            catch (Exception ex)
            {
                failureCount++;
                _logger.LogError(ex, "Failed to handle change.");

                span.SetStatus(Status.Error.WithDescription(ex.Message));
            }
        }

        span.SetAttribute("Document1.count", document1List.Count);
        span.SetAttribute("Document2.count", document2List.Count);
        span.SetAttribute("Unknown.count", unknownDocumentsList.Count);

        await Task.WhenAll(
            ProcessDocument1Async(document1List, cancellationToken),
            ProcessDocument2Async(document2List, cancellationToken),
            ProcessUnknownDocumentsAsync(unknownDocumentsList, cancellationToken)
        );

        span.SetAttribute("failure.count", failureCount);
        span.SetAttribute("success.count", changes.Count - failureCount);
        if (failureCount > 0)
        {
            span.SetStatus(Status.Error.WithDescription("One or more changes failed to process."));
        }
        else
        {
            span.SetStatus(Status.Ok);
        }
    }

    private async Task ProcessUnknownDocumentsAsync(List<object> unknownDocumentsList, CancellationToken cancellationToken)
    {
        try
        {
            await _deadletterService.DeadletterUnknownDocumentsAsync(unknownDocumentsList, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex, "Error while deadlettering {DocumentCount} documents.", unknownDocumentsList.Count
            );
            throw;
        }
    }

    private async Task ProcessDocument1Async(List<Document1> document1List, CancellationToken cancellationToken)
    {
        try
        {
            await ProcessDocumentsAsync(document1List, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex, "Error while processing {DocumentCount} Document1 documents.", document1List.Count
            );
            throw;
        }
    }

    private async Task ProcessDocument2Async(List<Document2> document2List, CancellationToken cancellationToken)
    {
        try
        {
            await ProcessDocumentsAsync(document2List, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex, "Error while processing {DocumentCount} Document2 documents.", document2List.Count
            );
            throw;
        }
    }

    private async Task ProcessDocumentsAsync<T>(List<T> document1List, CancellationToken cancellationToken)
    {
        var batch = await _eventHubProducerClient.CreateBatchAsync(cancellationToken);
        foreach (var document in document1List)
        {
            var eventData = new EventData(new BinaryData(JsonSerializer.Serialize(document)));
            if (!batch.TryAdd(eventData))
            {
                await _eventHubProducerClient.SendAsync(batch, cancellationToken);
                batch = await _eventHubProducerClient.CreateBatchAsync(cancellationToken);
                batch.TryAdd(eventData);
            }
        }

        if (batch.Count > 0)
        {
            await _eventHubProducerClient.SendAsync(batch, cancellationToken);
        }
    }
}
