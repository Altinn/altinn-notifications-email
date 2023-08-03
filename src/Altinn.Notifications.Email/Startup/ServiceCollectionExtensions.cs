using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Integrations.Clients;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;

namespace Altinn.Notifications.Email.Startup;

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
        services.AddSingleton<IEmailService, EmailService>();

        return services;
    }

    /// <summary>
    /// Add necessary integration services and configuration to the service collection.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="config">The application configuration.</param>
    /// <returns>The given service collection.</returns>
    public static IServiceCollection AddIntegrationServices(this IServiceCollection services, IConfiguration config)
    {
        services.AddSingleton<IEmailServiceClient, EmailServiceClient>();

        KafkaSettings kafkaSettings = new();
        config.GetSection(nameof(KafkaSettings)).Bind(kafkaSettings);

        services.AddSingleton(kafkaSettings);
        services.AddHostedService<EmailSendingConsumer>();

        return services;
    }
}
