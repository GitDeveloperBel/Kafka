using Serilog;
using Serilog.Core;
using Serilog.Sinks.SystemConsole.Themes;

namespace Serilogger;

public static class SeriloggerService
{

    public static ILogger GenerateLogger()
    {
        var levelSwitch = new LoggingLevelSwitch();
        var environment = "";
#if !RELEASE
        environment = "DEBUG";
        levelSwitch.MinimumLevel = Serilog.Events.LogEventLevel.Debug;
#else
        environment = "RELEASE";
        levelSwitch.MaximumLevel = Serilog.Events.LogEventLevel.Information;
#endif
        var config = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .MinimumLevel.ControlledBy(levelSwitch)
            .WriteTo.Console(theme: AnsiConsoleTheme.Code, outputTemplate: "[{Timestamp:HH:mm} {Level:u3}]: {Message:lj}{NewLine}{Exception}");

        return config.CreateLogger().ForContext("Environment", environment);
    }
}
