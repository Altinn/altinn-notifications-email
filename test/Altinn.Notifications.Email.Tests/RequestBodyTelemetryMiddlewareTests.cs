using System.Text;

using Altinn.Notifications.Email.Telemetry;

using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.AspNetCore.Http;

using Xunit;

namespace Altinn.Notifications.Email.Tests.Email.Telemetry;

public class RequestBodyTelemetryMiddlewareTests
{
    // Updated to match the exact schema that Azure SDK expects
    private const string _realWorldDeliveryEvent = "[{\"id\":\"e000f000-0000-0000-0000-000000000000\",\"topic\":\"/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/microsoft.communication/communicationservices/{acs-resource-name}\",\"subject\":\"sender/sender@mydomain.com/message/f000e000-0000-0000-0000-000000000000\",\"eventType\":\"Microsoft.Communication.EmailDeliveryReportReceived\",\"data\":{\"sender\":\"sender@mydomain.com\",\"recipient\":\"recipient@example.com\",\"messageId\":\"f000e000-0000-0000-0000-000000000000\",\"status\":\"Delivered\",\"deliveryAttemptTimeStamp\":\"2025-11-11T13:58:00.0000000Z\",\"deliveryStatusDetails\":{\"statusMessage\":\"No error.\"}},\"dataVersion\":\"1.0\",\"metadataVersion\":\"1\",\"eventTime\":\"2025-11-11T13:58:00Z\"}]";

    [Fact]
    public async Task InvokeAsync_PostWithRealWorldDeliveryEvent_ExtractsOperationId()
    {
        // Arrange
        var middleware = new RequestBodyTelemetryMiddleware(next: (innerHttpContext) => Task.CompletedTask);
        var context = CreateHttpContext("POST", _realWorldDeliveryEvent);
        var requestTelemetry = new RequestTelemetry();
        context.Features.Set(requestTelemetry);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        Assert.True(requestTelemetry.Properties.ContainsKey("OperationIds"), "OperationIds key should exist");
        Assert.Equal("f000e000-0000-0000-0000-000000000000", requestTelemetry.Properties["OperationIds"]);
        Assert.Equal("1", requestTelemetry.Properties["OperationCount"]);
    }

    [Fact]
    public async Task InvokeAsync_PostWithInvalidJson_DoesNotExtractOperationIds()
    {
        // Arrange
        var middleware = new RequestBodyTelemetryMiddleware(next: (innerHttpContext) => Task.CompletedTask);
        var context = CreateHttpContext("POST", "{invalid:json}");
        var requestTelemetry = new RequestTelemetry();
        context.Features.Set(requestTelemetry);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        Assert.False(requestTelemetry.Properties.ContainsKey("OperationIds"));
        Assert.False(requestTelemetry.Properties.ContainsKey("OperationCount"));
    }

    [Fact]
    public async Task InvokeAsync_PostWithEmptyBody_DoesNotExtractOperationIds()
    {
        // Arrange
        var middleware = new RequestBodyTelemetryMiddleware(next: (innerHttpContext) => Task.CompletedTask);
        var context = CreateHttpContext("POST", string.Empty);
        var requestTelemetry = new RequestTelemetry();
        context.Features.Set(requestTelemetry);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        Assert.False(requestTelemetry.Properties.ContainsKey("OperationIds"));
        Assert.False(requestTelemetry.Properties.ContainsKey("OperationCount"));
    }

    private static DefaultHttpContext CreateHttpContext(string method, string body)
    {
        var context = new DefaultHttpContext();
        context.Request.Method = method;
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body));
        context.Request.ContentType = "application/json";
        return context;
    }
}
