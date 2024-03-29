﻿using Altinn.Notifications.Email.Core.Sending;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Altinn.Notifications.Email.Core.Configuration;

/// <summary>
/// This class is responsible for holding extension methods for program startup.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Add necessary core services and configuration to the service collection.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="config">The application configuration.</param>
    /// <returns>The given service collection.</returns>
    public static IServiceCollection AddCoreServices(this IServiceCollection services, IConfiguration config)
    {
        TopicSettings topicSettings = config!.GetSection("KafkaSettings").Get<TopicSettings>()!;

        if (topicSettings == null)
        {
            throw new ArgumentNullException(nameof(config), "Required Kafka topic settings is missing from application configuration");
        }

        services.AddSingleton<ISendingService, SendingService>()
                .AddSingleton<IStatusService, StatusService>()
                .AddSingleton<IDateTimeService, DateTimeService>()
                .AddSingleton(topicSettings);

        return services;
    }
}
