using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;

using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Integrations.Kafka.Consumers
{
    /// <summary>
    /// Abstract base class for Kafka consumers, offering a robust framework for consuming messages from a specified topic.
    /// </summary>
    public abstract class KafkaConsumerBase : BackgroundService
    {
        private readonly string _topicName;
        private readonly ILogger<KafkaConsumerBase> _logger;
        private readonly IConsumer<string, string> _consumer;

        private int _shutdownInitiatedCounter;
        private int _processingFailureSignaledCounter;

        private readonly int _maxPollDurationMs = 100;
        private readonly int _polledConsumeResultsSize = 100;

        private readonly ConcurrentDictionary<TopicPartition, long> _latestProcessedOffsetByPartition = new();

        private static readonly Meter _meter = new("Altinn.Notifications.KafkaConsumer", "1.0.0");
        private static readonly Counter<int> _polledCounter = _meter.CreateCounter<int>("kafka.consumer.polled");
        private static readonly Counter<int> _processedCounter = _meter.CreateCounter<int>("kafka.consumer.processed");
        private static readonly Counter<int> _retriedFailedCounter = _meter.CreateCounter<int>("kafka.consumer.retried.failed");
        private static readonly Counter<int> _retriedSucceededCounter = _meter.CreateCounter<int>("kafka.consumer.retried.succeeded");
        private static readonly Histogram<double> _batchLatencyMs = _meter.CreateHistogram<double>("kafka.consumer.batch.latency.ms");

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaConsumerBase"/> class.
        /// </summary>
        protected KafkaConsumerBase(string topicName, KafkaSettings settings, ILogger<KafkaConsumerBase> logger)
        {
            _logger = logger;
            _topicName = topicName;

            var configuration = BuildConfiguration(settings);
            _consumer = BuildConsumer(configuration);
        }

        /// <inheritdoc/>
        public override void Dispose()
        {
            try
            {
                _consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// KafkaConsumer Dispose // Close failed");
            }
            finally
            {
                _consumer.Dispose();
                base.Dispose();
            }
        }

        /// <inheritdoc/>
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(_topicName);

            _logger.LogInformation("// {Class} // subscribed to topic {Topic}", GetType().Name, ComputeTopicFingerprint(_topicName));

            return base.StartAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            SignalShutdownIsInitiated();

            _consumer.Unsubscribe();

            _logger.LogInformation("// {Class} // unsubscribed from topic {Topic} because shutdown is initiated ", GetType().Name, ComputeTopicFingerprint(_topicName));

            var offsetsToCommit = _latestProcessedOffsetByPartition.Select(e => new TopicPartitionOffset(e.Key, new Offset(e.Value)));
            if (offsetsToCommit.Any())
            {
                try
                {
                    _consumer.Commit(offsetsToCommit);

                    _logger.LogInformation("// {Class} // Committed offsets for processed messages during shutdown", GetType().Name);
                }
                catch (KafkaException ex)
                {
                    _logger.LogError(ex, "// {Class} // Failed to commit offsets for processed messages during shutdown", GetType().Name);
                }
            }

            _consumer.Close();

            await base.StopAsync(cancellationToken);
        }

        /// <inheritdoc/>
        protected override abstract Task ExecuteAsync(CancellationToken stoppingToken);

        /// <summary>
        /// Consumes messages from the configured Kafka topic using batch polling and bounded parallel processing.
        /// </summary>
        /// <param name="processMessageFunc">
        /// Delegate that processes a single message value. Exceptions trigger a retry via <paramref name="retryMessageFunc"/>.
        /// </param>
        /// <param name="retryMessageFunc">
        /// Delegate invoked when <paramref name="processMessageFunc"/> fails. If it also fails, the batch stops launching new processing tasks.
        /// </param>
        /// <param name="cancellationToken">
        /// Token observed for cooperative cancellation. When signaled, polling and new task launches stop and in-flight tasks are awaited.
        /// </param>
        protected async Task ConsumeMessageAsync(Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && !IsShutdownInitiated)
            {
                var batchStopwatch = Stopwatch.StartNew();

                ResetMessageProcessingFailureSignal();

                var batchProcessingContext = PollConsumeResults(cancellationToken);
                if (batchProcessingContext.PolledConsumeResults.Count == 0)
                {
                    batchStopwatch.Stop();
                    await Task.Delay(10, cancellationToken);
                    continue;
                }

                _polledCounter.Add(batchProcessingContext.PolledConsumeResults.Count, KeyValuePair.Create<string, object?>("topic", ComputeTopicFingerprint(_topicName)));

                batchProcessingContext = await ProcessConsumeResultsAsync(batchProcessingContext, processMessageFunc, retryMessageFunc, cancellationToken);

                var contiguousCommitOffsets = ComputeContiguousCommitOffsets(batchProcessingContext);
                if (contiguousCommitOffsets.Count > 0)
                {
                    CommitOffsets(contiguousCommitOffsets);
                }

                batchStopwatch.Stop();

                _batchLatencyMs.Record(batchStopwatch.Elapsed.TotalMilliseconds, KeyValuePair.Create<string, object?>("topic", _topicName));
            }
        }

        /// <summary>
        /// Computes a deterministic truncated SHA-256 hexadecimal fingerprint for a Kafka topic name.
        /// The fingerprint is intended for log correlation and diagnostics without exposing the raw topic identifier.
        /// </summary>
        /// <param name="topicName">
        /// The original Kafka topic name to fingerprint. If <c>null</c>, empty, or whitespace,
        /// the literal string <c>"EMPTY"</c> is returned.
        /// </param>
        /// <returns>
        /// A 16 character lowercase hexadecimal string representing the first 8 bytes of the SHA-256 hash
        /// of <paramref name="topicName"/>, or <c>"EMPTY"</c> if the input is blank.
        /// </returns>
        private static string ComputeTopicFingerprint(string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName))
            {
                return "EMPTY";
            }

            ReadOnlySpan<byte> topicNameBytes = Encoding.UTF8.GetBytes(topicName);

            byte[] digest = SHA256.HashData(topicNameBytes);
            string hex = Convert.ToHexString(digest.AsSpan(0, 8));

            return hex.ToLowerInvariant();
        }

        /// <summary>
        /// Builds the Kafka <see cref="ConsumerConfig"/> using the shared client configuration.
        /// </summary>
        /// <param name="settings">The configuration object used to hold integration settings for Kafka.</param>
        /// <returns>A fully initialized <see cref="ConsumerConfig"/> ready to be used by a <see cref="ConsumerBuilder{TKey, TValue}"/>.</returns>
        private ConsumerConfig BuildConfiguration(KafkaSettings settings)
        {
            var configuration = new SharedClientConfig(settings);

            var consumerConfig = new ConsumerConfig(configuration.ConsumerConfig)
            {
                FetchWaitMaxMs = 100,
                QueuedMinMessages = 100,
                SessionTimeoutMs = 30000,
                EnableAutoCommit = false,
                FetchMinBytes = 512 * 1024,
                MaxPollIntervalMs = 300000,
                HeartbeatIntervalMs = 5000,
                EnableAutoOffsetStore = false,
                QueuedMaxMessagesKbytes = 16384,
                MaxPartitionFetchBytes = 4 * 1024 * 1024,
                SocketReceiveBufferBytes = 2 * 1024 * 1024,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = $"{settings.Consumer.GroupId}-{GetType().Name.ToLower()}"
            };

            return consumerConfig;
        }

        /// <summary>
        /// Commits offsets to Kafka safely by normalizing per-partition offsets and handling transient
        /// consumer group states. Normalization ensures only the highest next-offset per partition is committed.
        /// </summary>
        /// <param name="offsets">
        /// The per-message next-offsets (original offset + 1) to commit. May contain multiple entries per partition.
        /// </param>
        private void CommitOffsets(IEnumerable<TopicPartitionOffset> offsets)
        {
            if (offsets is null || IsShutdownInitiated)
            {
                return;
            }

            var normalizedOffsets = offsets
                .GroupBy(e => e.TopicPartition)
                .Select(e =>
                {
                    var maxOffset = e.Select(x => x.Offset.Value).Max();
                    return new TopicPartitionOffset(e.Key, new Offset(maxOffset));
                })
                .ToList();

            if (normalizedOffsets.Count == 0)
            {
                return;
            }

            try
            {
                _consumer.Commit(normalizedOffsets);
            }
            catch (KafkaException ex) when (ex.Error.Code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration)
            {
                _logger.LogWarning(ex, "// {Class} // Bulk commit skipped due to transient state: {Reason}", GetType().Name, ex.Error.Reason);
            }
            catch (KafkaException ex)
            {
                _logger.LogError(ex, "// {Class} // Bulk commit failed unexpectedly", GetType().Name);
            }
        }

        /// <summary>
        /// Indicates whether the consumer shutdown has been initiated.
        /// </summary>
        private bool IsShutdownInitiated => Volatile.Read(ref _shutdownInitiatedCounter) != 0;

        /// <summary>
        /// Creates and configures a Kafka consumer instance.
        /// </summary>
        /// <param name="consumerConfig">The <see cref="ConsumerConfig"/> used to build the consumer.</param>
        /// <returns>A configured <see cref="IConsumer{TKey, TValue}"/> for consuming messages with string keys and values.</returns>
        private IConsumer<string, string> BuildConsumer(ConsumerConfig consumerConfig)
        {
            return new ConsumerBuilder<string, string>(consumerConfig)
                .SetErrorHandler((_, e) =>
                {
                    if (e.IsFatal)
                    {
                        _logger.LogCritical("FATAL Kafka error. Code={ErrorCode}. Reason={Reason}", e.Code, e.Reason);
                    }
                    else if (e.IsError)
                    {
                        _logger.LogError("Kafka error. Code={ErrorCode}. Reason={Reason}", e.Code, e.Reason);
                    }
                    else
                    {
                        _logger.LogWarning("Kafka warning. Code={ErrorCode}. Reason={Reason}", e.Code, e.Reason);
                    }
                })
                .SetStatisticsHandler((_, json) =>
                {
                    _logger.LogDebug("// KafkaConsumerBase // Stats: {StatsJson}", json);
                })
                .SetPartitionsRevokedHandler((_, partitions) =>
                {
                    SignalMessageProcessingFailure();

                    var processedOffset = partitions
                    .Select(partition => _latestProcessedOffsetByPartition.TryGetValue(partition.TopicPartition, out var next) ? new TopicPartitionOffset(partition.TopicPartition, new Offset(next)) : null)
                    .Where(offset => offset != null);

                    if (processedOffset.Any())
                    {
                        try
                        {
                            _consumer.Commit(processedOffset);
                        }
                        catch (KafkaException ex) when (ex.Error.Code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration)
                        {
                            _logger.LogWarning(ex, "// {Class} // Commit on revocation skipped due to transient state: {Reason}", GetType().Name, ex.Error.Reason);
                        }
                        catch (KafkaException ex)
                        {
                            _logger.LogError(ex, "// {Class} // Commit on revocation failed unexpectedly", GetType().Name);
                        }
                    }

                    foreach (var partition in partitions)
                    {
                        _latestProcessedOffsetByPartition.TryRemove(partition.TopicPartition, out var _);
                    }

                    _logger.LogInformation("// {Class} // Partitions revoked: {Partitions}", GetType().Name, string.Join(',', partitions.Select(e => e.Partition.Value)));
                })
                .SetPartitionsAssignedHandler((_, partitions) =>
                {
                    _logger.LogInformation("// {Class} // Partitions assigned: {Partitions}", GetType().Name, string.Join(',', partitions.Select(e => e.Partition.Value)));
                })
                .Build();
        }

        /// <summary>
        /// Polls the Kafka consumer for new messages until either the time budget or the per-batch item cap is reached, or shutdown/cancellation is observed.
        /// </summary>
        /// <param name="cancellationToken">Token observed for cooperative cancellation and shutdown.</param>
        /// <returns>
        /// A <see cref="BatchProcessingContext"/> with <see cref="BatchProcessingContext.PolledConsumeResults"/>
        /// containing the consecutively polled <see cref="ConsumeResult{TKey, TValue}"/> items. The list may be empty.
        /// </returns>
        private BatchProcessingContext PollConsumeResults(CancellationToken cancellationToken)
        {
            var deadlineTickMs = Environment.TickCount64 + _maxPollDurationMs;
            var polledConsumeResults = new List<ConsumeResult<string, string>>(_polledConsumeResultsSize);

            while (!cancellationToken.IsCancellationRequested && !IsShutdownInitiated)
            {
                var remainingMs = (int)Math.Max(0, deadlineTickMs - Environment.TickCount64);
                if (remainingMs <= 0)
                {
                    break;
                }

                if (polledConsumeResults.Count >= _polledConsumeResultsSize)
                {
                    break;
                }

                try
                {
                    var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(remainingMs));
                    if (consumeResult is null)
                    {
                        break;
                    }

                    polledConsumeResults.Add(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "// {Class} // Exception during polling", GetType().Name);
                    break;
                }
            }

            return new BatchProcessingContext
            {
                PolledConsumeResults = [.. polledConsumeResults]
            };
        }

        /// <summary>
        /// Atomically signals that consumer shutdown has been initiated.
        /// </summary>
        private void SignalShutdownIsInitiated() => Interlocked.Exchange(ref _shutdownInitiatedCounter, 1);

        /// <summary>
        /// Indicates whether a message processing failure has occurred in the current batch.
        /// </summary>
        private bool IsMessageProcessingFailureSignaled => Volatile.Read(ref _processingFailureSignaledCounter) != 0;

        /// <summary>
        /// Computes per-partition commit offsets by determining the largest contiguous
        /// sequence of successfully processed messages from the earliest offset in each partition within the launched batch.
        /// </summary>
        /// <param name="batchContext">
        /// The batch context providing launched consume results and the successful next-offsets (offset + 1).
        /// </param>
        /// <returns>
        /// A list of <see cref="TopicPartitionOffset"/> values that are safe to commit to Kafka,
        /// representing the highest contiguous offset that can be committed for each partition without gaps.
        /// Returns an empty list if no contiguous sequences can be established.
        /// </returns>
        private static List<TopicPartitionOffset> ComputeContiguousCommitOffsets(BatchProcessingContext batchContext)
        {
            var batchByTopicPartition = batchContext.PolledConsumeResults
                .GroupBy(e => e.TopicPartition)
                .ToDictionary(e => e.Key, e => e.Select(x => x.Offset.Value).OrderBy(x => x).ToList());

            var successesByTopicPartition = batchContext.SuccessfulNextOffsets
                .GroupBy(e => e.TopicPartition)
                .ToDictionary(e => e.Key, e => new HashSet<long>(e.Select(s => s.Offset.Value)));

            var topicPartitionOffsetsToCommit = new List<TopicPartitionOffset>();

            foreach (var kvp in batchByTopicPartition)
            {
                var topicPartition = kvp.Key;
                var orderedOffsets = kvp.Value;

                if (!successesByTopicPartition.TryGetValue(topicPartition, out var successSet) || successSet.Count == 0)
                {
                    continue;
                }

                long? lastContiguousNext = null;

                foreach (var offset in orderedOffsets)
                {
                    var nextPosition = offset + 1;

                    if (successSet.Contains(nextPosition))
                    {
                        lastContiguousNext = nextPosition;
                    }
                    else
                    {
                        break;
                    }
                }

                if (lastContiguousNext.HasValue)
                {
                    topicPartitionOffsetsToCommit.Add(new TopicPartitionOffset(topicPartition, new Offset(lastContiguousNext.Value)));
                }
            }

            return topicPartitionOffsetsToCommit;
        }

        /// <summary>
        /// Atomically signals that a message processing failure has occurred in the current batch.
        /// </summary>
        private void SignalMessageProcessingFailure() => Interlocked.Exchange(ref _processingFailureSignaledCounter, 1);

        /// <summary>
        /// Atomically clears the message processing failure signal before handling a new batch.
        /// </summary>
        private void ResetMessageProcessingFailureSignal() => Interlocked.Exchange(ref _processingFailureSignaledCounter, 0);

        /// <summary>
        /// Launches processing tasks using Parallel.ForEachAsync for efficient concurrent processing with built-in resource management.
        /// Replaces manual task orchestration with Task.WhenAll() and provides better concurrency control.
        /// </summary>
        /// <param name="processingContext">
        /// The current batch context containing the polled consume results to be processed.
        /// </param>
        /// <param name="processMessageFunc">
        /// Delegate that processes a single message value. Exceptions trigger a retry via <paramref name="retryMessageFunc"/>.
        /// </param>
        /// <param name="retryMessageFunc">
        /// Delegate invoked when <paramref name="processMessageFunc"/> fails. If it also fails, the batch stops launching new processing tasks.
        /// </param>
        /// <param name="cancellationToken">
        /// Token observed for cooperative cancellation. When signaled, polling and new task launches stop and in-flight tasks are awaited.
        /// </param>
        /// <returns>
        /// An updated <see cref="BatchProcessingContext"/> with <see cref="BatchProcessingContext.SuccessfulNextOffsets"/> populated from completed tasks.
        /// </returns>
        private async Task<BatchProcessingContext> ProcessConsumeResultsAsync(BatchProcessingContext processingContext, Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested && IsShutdownInitiated)
            {
                return processingContext;
            }

            var parallelOptions = new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = _polledConsumeResultsSize
            };

            var successfulOffsets = new ConcurrentBag<TopicPartitionOffset>();

            try
            {
                await Parallel.ForEachAsync(
                    processingContext.PolledConsumeResults,
                    parallelOptions,
                    async (consumeResult, cancellationToken) =>
                    {
                        var result = await ProcessSingleMessageAsync(consumeResult, processMessageFunc, retryMessageFunc, cancellationToken);

                        if (result is not null)
                        {
                            successfulOffsets.Add(result);
                        }
                    });
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
            }

            _latestProcessedOffsetByPartition.Clear();
            foreach (var successfulOffset in successfulOffsets)
            {
                _latestProcessedOffsetByPartition.AddOrUpdate(successfulOffset.TopicPartition, successfulOffset.Offset.Value, (_, existing) => Math.Max(existing, successfulOffset.Offset.Value));
            }

            return processingContext with
            {
                SuccessfulNextOffsets = [.. successfulOffsets]
            };
        }

        /// <summary>
        /// Processes a single Kafka message with semaphore-controlled concurrency, retry logic, and comprehensive error handling.
        /// </summary>
        /// <param name="consumeResult">
        /// The consume result which cotains the Kafka-message to process.
        /// </param>
        /// <param name="processMessageFunc">
        /// Delegate that processes a single message value. Exceptions trigger a retry via <paramref name="retryMessageFunc"/>.
        /// </param>
        /// <param name="retryMessageFunc">
        /// Delegate invoked when <paramref name="processMessageFunc"/> fails. If it also fails, the batch stops launching new processing tasks.
        /// </param>
        /// <param name="cancellationToken">
        /// Token observed for cooperative cancellation. When signaled, polling and new task launches stop and in-flight tasks are awaited.
        /// </param>
        /// <returns>
        /// An updated <see cref="BatchProcessingContext"/> with <see cref="BatchProcessingContext.SuccessfulNextOffsets"/> populated from completed tasks.
        /// </returns>
        /// <returns>
        /// A <see cref="TopicPartitionOffset"/> representing the next offset to commit (original offset + 1)
        /// if message processing succeeds (either via primary or retry processing);
        /// otherwise <c>null</c> if processing fails or is short-circuited due to cancellation/shutdown.
        /// </returns>
        private async Task<TopicPartitionOffset?> ProcessSingleMessageAsync(ConsumeResult<string, string> consumeResult, Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            try
            {
                if (cancellationToken.IsCancellationRequested || IsMessageProcessingFailureSignaled || IsShutdownInitiated)
                {
                    return null;
                }

                await processMessageFunc(consumeResult.Message.Value);

                _processedCounter.Add(1, KeyValuePair.Create<string, object?>("topic", ComputeTopicFingerprint(_topicName)));

                return new TopicPartitionOffset(consumeResult.TopicPartition, consumeResult.Offset + 1);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "// {Class} // Error processing message at offset {Offset}, attempting retry", GetType().Name, consumeResult.Offset);

                try
                {
                    if (cancellationToken.IsCancellationRequested || IsMessageProcessingFailureSignaled || IsShutdownInitiated)
                    {
                        return null;
                    }

                    await retryMessageFunc(consumeResult.Message.Value);

                    _retriedSucceededCounter.Add(1, KeyValuePair.Create<string, object?>("topic", ComputeTopicFingerprint(_topicName)));

                    return new TopicPartitionOffset(consumeResult.TopicPartition, consumeResult.Offset + 1);
                }
                catch (Exception retryEx)
                {
                    _logger.LogError(retryEx, "// {Class} // Retry failed for message at offset {Offset}. Halting further launches.", GetType().Name, consumeResult.Offset);

                    _retriedFailedCounter.Add(1, KeyValuePair.Create<string, object?>("topic", ComputeTopicFingerprint(_topicName)));

                    SignalMessageProcessingFailure();

                    return null;
                }
            }
        }
    }
}
