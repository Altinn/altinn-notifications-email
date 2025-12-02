using System.Diagnostics;
using System.Text;

using Altinn.Notifications.Email.Configuration;
using Altinn.Notifications.Email.Telemetry;

using Microsoft.AspNetCore.Http;
using Xunit;

namespace Altinn.Notifications.Email.Tests;

public class RequestBodyTelemetryMiddlewareTests
{
    private const string _realWorldDeliveryEvent = "[{\"id\":\"e000f000-0000-0000-0000-000000000000\",\"topic\":\"/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/microsoft.communication/communicationservices/{acs-resource-name}\",\"subject\":\"sender/sender@mydomain.com/message/f000e000-0000-0000-0000-000000000000\",\"eventType\":\"Microsoft.Communication.EmailDeliveryReportReceived\",\"data\":{\"sender\":\"sender@mydomain.com\",\"recipient\":\"recipient@example.com\",\"messageId\":\"f000e000-0000-0000-0000-000000000000\",\"status\":\"Delivered\",\"deliveryAttemptTimeStamp\":\"2025-11-11T13:58:00.0000000Z\",\"deliveryStatusDetails\":{\"statusMessage\":\"No error.\"}},\"dataVersion\":\"1.0\",\"metadataVersion\":\"1\",\"eventTime\":\"2025-11-11T13:58:00Z\"}]";
    private readonly Microsoft.Extensions.Options.IOptions<EmailDeliveryReportSettings> _options = Microsoft.Extensions.Options.Options.Create(new EmailDeliveryReportSettings());

    [Fact]
    public async Task InvokeAsync_ParseObjectSendOperationResults_AddsSendOperationResultsTag()
    {
        // Arrange
        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = CreateActivity();
        var options = CreateOptions("sendoperationresults");
        var middleware = new RequestBodyTelemetryMiddleware(
            next: (innerHttpContext) => Task.CompletedTask,
            emailDeliveryReportSettings: options);
        var context = CreateHttpContext("POST", _realWorldDeliveryEvent);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        Assert.NotNull(activity);
        var sendOperationResultsTag = activity.Tags.FirstOrDefault(t => t.Key == "SendOperationResults");
        Assert.NotEqual(default, sendOperationResultsTag);
        Assert.Contains("f000e000-0000-0000-0000-000000000000", sendOperationResultsTag.Value);
        Assert.Contains("2", sendOperationResultsTag.Value); // Delivered
    }

    [Fact]
    public async Task InvokeAsync_CallsNextMiddleware()
    {
        // Arrange
        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = CreateActivity();
        bool nextMiddlewareCalled = false;
        var middleware = new RequestBodyTelemetryMiddleware(
            next: (innerHttpContext) =>
            {
                nextMiddlewareCalled = true;
                return Task.CompletedTask;
            },
            emailDeliveryReportSettings: _options);
        var context = CreateHttpContext("POST", _realWorldDeliveryEvent);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        Assert.True(nextMiddlewareCalled);
    }

    [Fact]
    public async Task InvokeAsync_PreservesRequestBodyForNextMiddleware()
    {
        // Arrange
        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = CreateActivity();
        string? bodyReadByNextMiddleware = null;
        var middleware = new RequestBodyTelemetryMiddleware(
            next: async (innerHttpContext) =>
            {
                using var reader = new StreamReader(innerHttpContext.Request.Body);
                bodyReadByNextMiddleware = await reader.ReadToEndAsync();
            },
            emailDeliveryReportSettings: _options);
        var context = CreateHttpContext("POST", _realWorldDeliveryEvent);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        Assert.Equal(_realWorldDeliveryEvent, bodyReadByNextMiddleware);
    }

    private static DefaultHttpContext CreateHttpContext(string method, string body)
    {
        var context = new DefaultHttpContext();
        context.Request.Method = method;
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body));
        context.Request.ContentType = "application/json";
        return context;
    }

    private static Activity CreateActivity()
    {
        var activitySource = new ActivitySource("TestSource");
        var activity = activitySource.StartActivity("TestActivity");
        Activity.Current = activity;
        return activity!;
    }

    private static Microsoft.Extensions.Options.IOptions<EmailDeliveryReportSettings> CreateOptions(string parseObject)
    {
        return Microsoft.Extensions.Options.Options.Create(new EmailDeliveryReportSettings
        {
            ParseObject = parseObject
        });
    }
}
