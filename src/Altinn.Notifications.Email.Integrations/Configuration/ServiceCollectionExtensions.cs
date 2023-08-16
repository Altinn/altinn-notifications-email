using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Integrations.Clients;
using Altinn.Notifications.Email.Integrations.Consumers;
using Altinn.Notifications.Email.Integrations.Producers;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Altinn.Notifications.Email.Integrations.Configuration;

/// <summary>
/// This class is responsible for holding extension methods for program startup.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Add necessary integration services and configuration to the service collection.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="config">The application configuration.</param>
    /// <returns>The given service collection.</returns>
    public static IServiceCollection AddIntegrationServices(this IServiceCollection services, IConfiguration config)
    {
        CommunicationServicesSettings communicationServicesSettings = new();
        config.GetSection(nameof(CommunicationServicesSettings)).Bind(communicationServicesSettings);
        services.AddSingleton(communicationServicesSettings);
        services.AddSingleton<IEmailServiceClient, EmailServiceClient>();

        KafkaSettings kafkaSettings = new();
        config.GetSection(nameof(KafkaSettings)).Bind(kafkaSettings);
        services.AddSingleton(kafkaSettings);
        services.AddHostedService<EmailSendingConsumer>();

        services.AddSingleton<IEmailSendingAcceptedProducer, EmailSendingAcceptedProducer>();
        return services;
    }
}
