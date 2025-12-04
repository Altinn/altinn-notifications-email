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
    /// Abstract base class for Kafka consumers, providing a framework for consuming messages from a specified topic.
    /// Maximizes throughput with bounded parallelism while enforcing: do not start more messages after the first failure
    /// (including failed retry). Already-started messages finish; contiguous successes are committed.
    /// </summary>
    public abstract class KafkaConsumerBase : BackgroundService
    {
        private readonly string _topicName;
        private readonly ILogger<KafkaConsumerBase> _logger;
        private readonly IConsumer<string, string> _consumer;

        private bool _isShutdownInitiated;
        private bool _isProcessingFailureSignaled;

        private readonly int _maxPollDurationMs = 100;
        private readonly int _polledConsumeResultsSize = 50;

        private volatile IReadOnlyList<Task> _currentBatchTasks = [];

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
            IsShutdownInitiated = true;

            _consumer.Unsubscribe();

            _logger.LogInformation("// {Class} // unsubscribed from topic {Topic}", GetType().Name, ComputeTopicFingerprint(_topicName));

            while (!cancellationToken.IsCancellationRequested)
            {
                var pending = _currentBatchTasks.Where(e => !e.IsCompleted).ToArray() ?? [];
                if (pending.Length == 0)
                {
                    break;
                }

                try
                {
                    var waitAll = Task.WhenAll(pending);
                    var completed = await Task.WhenAny(waitAll, Task.Delay(TimeSpan.FromSeconds(10), cancellationToken));

                    if (completed == waitAll)
                    {
                        break;
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

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
        protected async Task ConsumeMessage(Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            while (cancellationToken.IsCancellationRequested && IsShutdownInitiated)
            {
                Interlocked.Exchange(ref _isProcessingFailureSignaled, false);

                var batchProcessingStopwatch = Stopwatch.StartNew();

                var batchProcessingContext = PollConsumeResults(cancellationToken);
                if (batchProcessingContext.PolledConsumeResults.Count == 0)
                {
                    batchProcessingStopwatch.Stop();

                    await Task.Delay(10, cancellationToken);

                    continue;
                }

                if (cancellationToken.IsCancellationRequested && IsShutdownInitiated)
                {
                    batchProcessingStopwatch.Stop();

                    break;
                }

                var processingTaskFactories = new List<Task<TopicPartitionOffset?>>(batchProcessingContext.PolledConsumeResults.Count);

                foreach (var consumeResult in batchProcessingContext.PolledConsumeResults)
                {
                    var messageProcessingTaskFactory = CreateMessageProcessingTaskFactory(consumeResult, processMessageFunc, retryMessageFunc, cancellationToken);

                    processingTaskFactories.Add(messageProcessingTaskFactory());
                }

                if (cancellationToken.IsCancellationRequested && IsShutdownInitiated)
                {
                    batchProcessingStopwatch.Stop();

                    break;
                }

                await Task.WhenAll(processingTaskFactories);

                var successfulNextOffsets = new ConcurrentBag<TopicPartitionOffset>();
                foreach (var processingTask in processingTaskFactories)
                {
                    var result = await processingTask;

                    if (result is not null)
                    {
                        successfulNextOffsets.Add(result);
                    }
                }

                batchProcessingContext = batchProcessingContext with
                {
                    SuccessfulNextOffsets = successfulNextOffsets
                };

                // Update context with all polled results and the successful ones
                var finalContext = batchProcessingContext with
                {
                    SuccessfulNextOffsets = successfulNextOffsets
                };

                // Compute and commit the highest contiguous offsets
                var offsetsToCommit = ComputeContiguousCommitOffsets(finalContext);
                if (offsetsToCommit.Count > 0)
                {
                    CommitNormalizedOffsets(offsetsToCommit);
                }
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

            Span<byte> digest = stackalloc byte[32];
            SHA256.HashData(topicNameBytes, digest);

            // First 8 bytes -> 16 hex chars (truncated fingerprint)
            Span<char> fingerprintBuffer = stackalloc char[16];
            const string hexAlphabet = "0123456789abcdef";

            for (int i = 0; i < 8; i++)
            {
                byte byteValue = digest[i];
                fingerprintBuffer[i * 2] = hexAlphabet[byteValue >> 4];
                fingerprintBuffer[(i * 2) + 1] = hexAlphabet[byteValue & 0x0F];
            }

            return new string(fingerprintBuffer);
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
                QueuedMinMessages = 50,
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
                    _logger.LogInformation("// {Class} // Partitions revoked: {Partitions}", GetType().Name, string.Join(',', partitions.Select(e => e.Partition.Value)));
                })
                .SetPartitionsAssignedHandler((_, partitions) =>
                {
                    _logger.LogInformation("// {Class} // Partitions assigned: {Partitions}", GetType().Name, string.Join(',', partitions.Select(e => e.Partition.Value)));
                })
                .Build();
        }

        /// <summary>
        /// Commits offsets to Kafka safely by normalizing per-partition offsets and handling transient
        /// consumer group states. Normalization ensures only the highest next-offset per partition is committed.
        /// </summary>
        /// <param name="offsets">
        /// The per-message next-offsets (original offset + 1) to commit. May contain multiple entries per partition.
        /// </param>
        private void CommitNormalizedOffsets(IEnumerable<TopicPartitionOffset> offsets)
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
        /// Computes per-partition commit offsets by determining the largest contiguous
        /// sequence of successfully processed messages from the earliest offset in each partition
        /// within the launched batch.
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
            var commitOffsets = new List<TopicPartitionOffset>();

            var batchByTopicPartition = batchContext.PolledConsumeResults
                .GroupBy(e => e.TopicPartition)
                .ToDictionary(e => e.Key, e => e.Select(x => x.Offset.Value).OrderBy(x => x).ToList());

            var successesByTopicPartition = batchContext.SuccessfulNextOffsets
                .GroupBy(e => e.TopicPartition)
                .ToDictionary(e => e.Key, e => new HashSet<long>(e.Select(s => s.Offset.Value)));

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
                    commitOffsets.Add(new TopicPartitionOffset(topicPartition, new Offset(lastContiguousNext.Value)));
                }
            }

            return commitOffsets;
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
            var batchPollingDeadline = DateTime.UtcNow.AddMilliseconds(_maxPollDurationMs);
            var polledConsumeResults = new List<ConsumeResult<string, string>>(_polledConsumeResultsSize);

            while (!cancellationToken.IsCancellationRequested && !IsShutdownInitiated)
            {
                var remainingPollingTimeSpan = batchPollingDeadline - DateTime.UtcNow;
                if (remainingPollingTimeSpan <= TimeSpan.Zero)
                {
                    break;
                }

                if (polledConsumeResults.Count >= _polledConsumeResultsSize)
                {
                    break;
                }

                try
                {
                    var consumeResult = _consumer.Consume(remainingPollingTimeSpan);
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
                PolledConsumeResults = polledConsumeResults
            };
        }

        /// <summary>
        /// Indicates whether this consumer instance has initiated shutdown.
        /// </summary>
        /// <value>
        /// <c>true</c> if shutdown has been initiated; otherwise, <c>false</c>.
        /// </value>
        private bool IsShutdownInitiated
        {
            get => Volatile.Read(ref _isShutdownInitiated);
            set => Interlocked.Exchange(ref _isShutdownInitiated, value);
        }

        /// <summary>
        /// Indicates whether a message processing failure has been signaled from any processing tasks.
        /// </summary>
        private bool IsMessageProcessingFailureSignaled => Volatile.Read(ref _isProcessingFailureSignaled);

        /// <summary>
        /// Atomically signals a message processing failure.
        /// </summary>
        private void SignalMessageProcessingFailure() => Interlocked.Exchange(ref _isProcessingFailureSignaled, true);

        /// <summary>
        /// Builds a deferred-start factory for processing a single Kafka message.
        /// </summary>
        /// <remarks>
        /// - The returned delegate, when invoked, executes <paramref name="processMessageFunc"/>; on failure, it executes <paramref name="retryMessageFunc"/>.
        /// - Returns <c>true</c> when the message is handled successfully (either initial processing or retry); otherwise <c>false</c>.
        /// - Honors batch-level stop conditions: if shutdown is initiated or a processing failure has been signaled, the task short-circuits and returns <c>false</c>.
        /// </remarks>
        /// <param name="consumeResult">The Kafka message to process.</param>
        /// <param name="processMessageFunc">Delegate that performs the primary handling of the message.</param>
        /// <param name="retryMessageFunc">Delegate invoked to retry handling the message when the primary handling fails.</param>
        /// <param name="cancellationToken">Token observed for cooperative cancellation prior to starting or during execution.</param>
        /// <returns>
        /// A factory delegate that, when invoked, starts processing and returns a <see cref="Task{Boolean}"/>: 
        /// <c>true</c> on successful handling; <c>false</c> if both processing and retry fail or launch is short-circuited.
        /// </returns>
        private Func<Task<TopicPartitionOffset?>> CreateMessageProcessingTaskFactory(ConsumeResult<string, string> consumeResult, Func<string, Task> processMessageFunc, Func<string, Task> retryMessageFunc, CancellationToken cancellationToken)
        {
            return async () =>
            {
                try
                {
                    if (cancellationToken.IsCancellationRequested || IsMessageProcessingFailureSignaled || IsShutdownInitiated)
                    {
                        return null;
                    }

                    await processMessageFunc(consumeResult.Message.Value);

                    return new TopicPartitionOffset(consumeResult.TopicPartition, consumeResult.Offset + 1);
                }
                catch (Exception)
                {
                    try
                    {
                        if (cancellationToken.IsCancellationRequested || IsMessageProcessingFailureSignaled || IsShutdownInitiated)
                        {
                            return null;
                        }

                        await retryMessageFunc(consumeResult.Message.Value);

                        return new TopicPartitionOffset(consumeResult.TopicPartition, consumeResult.Offset + 1);
                    }
                    catch (Exception)
                    {
                        SignalMessageProcessingFailure();
                        return null;
                    }
                }
            };
        }
    }
}
