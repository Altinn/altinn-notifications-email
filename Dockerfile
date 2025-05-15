# Use the official .NET SDK image with Alpine Linux as a base image
FROM mcr.microsoft.com/dotnet/sdk:9.0.203-alpine3.21@sha256:33be1326b4a2602d08e145cf7e4a8db4b243db3cac3bdec42e91aef930656080 AS build

# Set the working directory in the container
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY /src/Altinn.Notifications.Email/*.csproj ./src/Altinn.Notifications.Email/
COPY /src/Altinn.Notifications.Email.Core/*.csproj ./src/Altinn.Notifications.Email.Core/
COPY /src/Altinn.Notifications.Email.Integrations/*.csproj ./src/Altinn.Notifications.Email.Integrations/

RUN dotnet restore ./src/Altinn.Notifications.Email/Altinn.Notifications.Email.csproj

# Copy the remaining source code and build the application
COPY /src ./src
RUN dotnet publish -c Release -o out ./src/Altinn.Notifications.Email/Altinn.Notifications.Email.csproj


# Use the official .NET runtime image with Alpine Linux as a base image
FROM mcr.microsoft.com/dotnet/aspnet:9.0.4-alpine3.21@sha256:6f67d7bfa9a770ad2f1d62204d51de1afdc96783866b8d89cce34f0f8a69561e AS final
EXPOSE 5091
WORKDIR /app
COPY --from=build /app/out ./

# setup the user and group
# the user will have no password, using shell /bin/false and using the group dotnet
RUN addgroup -g 3000 dotnet && adduser -u 1000 -G dotnet -D -s /bin/false dotnet
# update permissions of files if neccessary before becoming dotnet user
USER dotnet
RUN mkdir /tmp/logtelemetry

# Run the application
ENTRYPOINT [ "dotnet", "Altinn.Notifications.Email.dll" ]
