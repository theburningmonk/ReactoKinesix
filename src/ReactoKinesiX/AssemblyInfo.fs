namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("ReactoKinesix")>]
[<assembly: AssemblyProductAttribute("ReactoKinesix")>]
[<assembly: AssemblyDescriptionAttribute("A Rx-based .Net client library for Amazon Kinesis")>]
[<assembly: AssemblyVersionAttribute("0.1.0")>]
[<assembly: AssemblyFileVersionAttribute("0.1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.1.0"
