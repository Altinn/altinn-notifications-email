using System.Reflection;

using Altinn.Notifications.Email.Configuration;
using Altinn.Notifications.Email.Core.Configuration;
using Altinn.Notifications.Email.Health;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Startup;

using Azure.Identity;
using Azure.Security.KeyVault.Secrets;

using Microsoft.ApplicationInsights.AspNetCore.Extensions;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights.WindowsServer.TelemetryChannel;
using Microsoft.Extensions.FileProviders;
using Swashbuckle.AspNetCore.Filters;
using Swashbuckle.AspNetCore.SwaggerGen;

ILogger logger;
const string AppInsightsKeyName = "ApplicationInsights--InstrumentationKey";
string applicationInsightsConnectionString = string.Empty;

WebApplicationBuilder appBuilder = WebApplication.CreateBuilder(args);

ConfigureWebHostCreationLogging();

await SetConfigurationProviders(appBuilder.Configuration);

appBuilder.Logging.ConfigureApplicationLogging(applicationInsightsConnectionString);

ConfigureServices(appBuilder.Services, appBuilder.Configuration);

appBuilder.Services.AddEndpointsApiExplorer();
appBuilder.Services.AddSwaggerGen(c =>
{
    IncludeXmlComments(c);
    c.EnableAnnotations();
    c.OperationFilter<AddResponseHeadersFilter>();
});

var app = appBuilder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

Configure();

app.Run();

void ConfigureWebHostCreationLogging()
{
    var logFactory = LoggerFactory.Create(logBuilder =>
    {
        logBuilder
            .AddFilter("Altinn.Notifications.Email.Program", LogLevel.Debug)
            .AddConsole();
    });

    logger = logFactory.CreateLogger<Program>();
}

async Task SetConfigurationProviders(ConfigurationManager config)
{
    if (Directory.Exists("/altinn-appsettings"))
    {
        logger.LogWarning("Reading altinn-dbsettings-secret.json.");
        IFileProvider fileProvider = new PhysicalFileProvider("/altinn-appsettings");
        config.AddJsonFile(fileProvider, "altinn-dbsettings-secret.json", optional: true, reloadOnChange: true);
    }
    else
    {
        logger.LogWarning("Expected directory \"/altinn-appsettings\" not found.");
    }

    await ConnectToKeyVaultAndSetApplicationInsights(config);
}

async Task ConnectToKeyVaultAndSetApplicationInsights(ConfigurationManager config)
{
    KeyVaultSettings keyVaultSettings = new();
    config.GetSection("kvSetting").Bind(keyVaultSettings);

    if (!string.IsNullOrEmpty(keyVaultSettings.ClientId) &&
        !string.IsNullOrEmpty(keyVaultSettings.TenantId) &&
        !string.IsNullOrEmpty(keyVaultSettings.ClientSecret) &&
        !string.IsNullOrEmpty(keyVaultSettings.SecretUri))
    {
        logger.LogInformation("Program // Configure key vault client // App");
        Environment.SetEnvironmentVariable("AZURE_CLIENT_ID", keyVaultSettings.ClientId);
        Environment.SetEnvironmentVariable("AZURE_CLIENT_SECRET", keyVaultSettings.ClientSecret);
        Environment.SetEnvironmentVariable("AZURE_TENANT_ID", keyVaultSettings.TenantId);
        var azureCredentials = new DefaultAzureCredential();

        config.AddAzureKeyVault(new Uri(keyVaultSettings.SecretUri), azureCredentials);

        SecretClient client = new(new Uri(keyVaultSettings.SecretUri), azureCredentials);

        KeyVaultSecret keyVaultSecret = await client.GetSecretAsync(AppInsightsKeyName);
        applicationInsightsConnectionString = string.Format("InstrumentationKey={0}", keyVaultSecret.Value);
    }
}

void ConfigureServices(IServiceCollection services, ConfigurationManager configuration)
{
    services.AddControllers();
    services.AddHealthChecks().AddCheck<HealthCheck>("notifications_emails_health_check");

    services.AddCoreServices(configuration);
    services.AddIntegrationServices(configuration);
        
    if (!string.IsNullOrEmpty(applicationInsightsConnectionString))
    {
        services.AddSingleton(typeof(ITelemetryChannel), new ServerTelemetryChannel { StorageFolder = "/tmp/logtelemetry" });
        services.AddApplicationInsightsTelemetry(new ApplicationInsightsServiceOptions
        {
            ConnectionString = applicationInsightsConnectionString
        });

        services.AddApplicationInsightsTelemetryProcessor<HealthTelemetryFilter>();
        services.AddSingleton<ITelemetryInitializer, CustomTelemetryInitializer>();
    }
}

void Configure()
{
    app.UseAuthorization();
    app.MapControllers();
    app.MapHealthChecks("/health");
}

void IncludeXmlComments(SwaggerGenOptions swaggerGenOptions)
{
    try
    {
        string xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
        string xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
        swaggerGenOptions.IncludeXmlComments(xmlPath);
    }
    catch (Exception e)
    {
        logger.LogWarning(e, "Program // Exception when attempting to include the XML comments file(s).");
    }
}
