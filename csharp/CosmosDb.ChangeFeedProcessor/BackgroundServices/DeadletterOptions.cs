namespace Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;

public class DeadletterOptions
{
    public string DatabaseId { get; set; } = default!;
    public string ContainerId { get; set; } = default!;
}