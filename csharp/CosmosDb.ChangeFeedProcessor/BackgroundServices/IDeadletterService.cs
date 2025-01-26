namespace Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;

public interface IDeadletterService
{
    Task DeadletterUnknownDocumentsAsync(List<object> documents, CancellationToken cancellationToken);
}
