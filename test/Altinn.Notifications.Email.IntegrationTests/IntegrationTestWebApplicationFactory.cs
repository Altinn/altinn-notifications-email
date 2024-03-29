﻿using Altinn.Notifications.Email.Integrations.Consumers;

using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;

namespace Altinn.Notifications.Email.IntegrationTests;

public class IntegrationTestWebApplicationFactory<TStartup> : WebApplicationFactory<TStartup>
      where TStartup : class
{
    /// <summary>
    /// ConfigureWebHost for setup of configuration and test services
    /// </summary>
    /// <param name="builder">IWebHostBuilder</param>
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureAppConfiguration(config =>
        {
            config.AddConfiguration(new ConfigurationBuilder()
                .Build());
        });

        builder.ConfigureTestServices(services =>
        {
            var descriptor = services.Single(s => s.ImplementationType == typeof(SendEmailQueueConsumer));
            services.Remove(descriptor);
            descriptor = services.Single(s => s.ImplementationType == typeof(EmailSendingAcceptedConsumer));
            services.Remove(descriptor);
        });
    }
}
