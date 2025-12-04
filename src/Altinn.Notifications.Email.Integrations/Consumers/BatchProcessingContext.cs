using Confluent.Kafka;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Encapsulates the state and results of a message batch processing operation for Kafka consumer.
/// </summary>
public sealed record BatchProcessingContext
{
    /// <summary>
    /// Thread-safe collection of per-message next offsets (original offset + 1) for successfully processed consume results.
    /// </summary>
    public IList<TopicPartitionOffset> SuccessfulNextOffsets { get; init; } = [];

    /// <summary>
    /// Consume results obtained during the poll phase for this batch.
    /// This may include items that were not launched, depending on failure/cancellation conditions.
    /// </summary>
    public IList<ConsumeResult<string, string>> PolledConsumeResults { get; init; } = [];
}
