namespace Axm.CosmosDb.ChangeFeedProcessor.BackgroundServices;

public class ChangeProcessorOptions
{
    public string DatabaseId { get; set; } = default!;
    public string ContainerId { get; set; } = default!;
    public int PollIntervalInSeconds { get; set; } = 1;
}
