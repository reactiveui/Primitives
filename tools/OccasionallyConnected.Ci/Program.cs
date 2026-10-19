using System.Globalization;
using OccasionallyConnected.Ci;

var cultureName = Environment.GetEnvironmentVariable("OC_CULTURE");
if (!string.IsNullOrWhiteSpace(cultureName))
{
    var culture = CultureInfo.GetCultureInfo(cultureName);
    CultureInfo.CurrentCulture = culture;
    CultureInfo.CurrentUICulture = culture;
}

if (args.Length == 0)
{
    await Console.Error.WriteLineAsync("Usage: occasionally-connected-ci <coverage|packages|supply-chain|mutation|aot|merge-mobile> [options]");
    return 2;
}

try
{
    var options = args[1..];
    return args[0] switch
    {
        "coverage" => Coverage.Run(options),
        "packages" => Packages.Run(options),
        "supply-chain" => await SupplyChain.Run(options),
        "mutation" => Mutation.Run(options),
        "aot" => Aot.Run(options),
        "merge-mobile" => MobileNativePackage.Run(options),
        _ => UnknownCommand(args[0]),
    };
}
catch (Exception error)
{
    await Console.Error.WriteLineAsync(error.ToString());
    return 1;
}

static int UnknownCommand(string command)
{
    Console.Error.WriteLine($"Unknown CI command: {command}");
    return 2;
}
