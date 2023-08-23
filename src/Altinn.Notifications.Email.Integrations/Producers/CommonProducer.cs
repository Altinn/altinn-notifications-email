﻿using Altinn.Notifications.Email.Core.Integrations.Interfaces;
using Altinn.Notifications.Email.Integrations.Configuration;

using Confluent.Kafka;
using Confluent.Kafka.Admin;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Producers;

/// <summary>
/// Implementation of a generic Kafka producer.
/// </summary>
public sealed class CommonProducer : ICommonProducer, IDisposable
{
    private readonly IProducer<Null, string> _producer;
    private readonly KafkaSettings _kafkaSettings;
    private readonly SharedClientConfig _sharedClientConfig;
    private readonly ILogger<CommonProducer> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="ICommonProducer"/> class.
    /// </summary>
    public CommonProducer(KafkaSettings kafkaSettings, ILogger<CommonProducer> logger)
    {
        _kafkaSettings = kafkaSettings;
        _logger = logger;

        _sharedClientConfig = new SharedClientConfig(kafkaSettings);

        var config = new ProducerConfig(_sharedClientConfig.ClientConfig)
        {
            Acks = Acks.All,
            EnableDeliveryReports = true,
            EnableIdempotence = true,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();

        EnsureTopicsExist();
    }

    /// <inheritdoc/>
    public async Task<bool> ProduceAsync(string topic, string message)
    {
        try
        {
            DeliveryResult<Null, string> result = await _producer.ProduceAsync(topic, new Message<Null, string>
            {
                Value = message
            });

            if (result.Status != PersistenceStatus.Persisted)
            {
                _logger.LogError("// KafkaProducer // ProduceAsync // Message not ack'd by all brokers (value: '{message}'). Delivery status: {result.Status}", message, result.Status);
                return false;
            }
        }
        catch (ProduceException<long, string> ex)
        {
            _logger.LogError(ex, "// KafkaProducer // ProduceAsync // Permanent error: {Message} for message (value: '{DeliveryResult}')", ex.Message, ex.DeliveryResult.Value);
            throw;
        }

        return true;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _producer?.Flush();
        _producer?.Dispose();
        GC.SuppressFinalize(this);
    }

    private void EnsureTopicsExist()
    {
        using var adminClient = new AdminClientBuilder(_sharedClientConfig.AdminClientConfig).Build();
        var existingTopics = adminClient.GetMetadata(TimeSpan.FromSeconds(10)).Topics;

        foreach (string topic in _kafkaSettings.TopicList)
        {
            if (!existingTopics.Exists(t => t.Topic.Equals(topic, StringComparison.OrdinalIgnoreCase)))
            {
                try
                {
                    adminClient.CreateTopicsAsync(new TopicSpecification[]
                    {
                        new TopicSpecification()
                        {
                            Name = topic,
                            NumPartitions = _sharedClientConfig.TopicSpecification.NumPartitions,
                            ReplicationFactor = _sharedClientConfig.TopicSpecification.ReplicationFactor
                        }
                    }).Wait();
                    _logger.LogInformation("// KafkaProducer // EnsureTopicsExists // Topic '{Topic}' created successfully.", topic);
                }
                catch (CreateTopicsException ex)
                {
                    _logger.LogError(ex, "// KafkaProducer // EnsureTopicsExists // Failed to create topic '{Topic}'", topic);
                    throw;
                }
            }
        }
    }
}