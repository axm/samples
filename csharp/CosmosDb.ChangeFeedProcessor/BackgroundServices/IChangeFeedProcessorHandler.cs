namespace Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;

public interface IChangeFeedProcessorHandler
{
    Task HandleAsync(IReadOnlyCollection<object> changes, CancellationToken cancellationToken);
}
