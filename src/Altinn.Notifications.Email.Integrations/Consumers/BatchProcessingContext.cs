using System.Collections.Immutable;

using Confluent.Kafka;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Encapsulates the state and results of a message batch processing operation for Kafka consumer.
/// </summary>
public sealed record BatchProcessingContext
{
    /// <summary>
    /// Per-message next offsets (original offset + 1) for successfully processed consume results.
    /// </summary>
    public IImmutableList<TopicPartitionOffset> SuccessfulNextOffsets { get; init; } = [];

    /// <summary>
    /// Consume results obtained during the poll phase for this batch.
    /// </summary>
    public IImmutableList<ConsumeResult<string, string>> PolledConsumeResults { get; init; } = [];
}
