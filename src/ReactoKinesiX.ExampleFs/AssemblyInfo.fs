namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: AssemblyTitleAttribute("ReactoKinesix.ExampleFs")>]
[<assembly: AssemblyProductAttribute("ReactoKinesix")>]
[<assembly: AssemblyDescriptionAttribute("A Rx-based .Net client library for Amazon Kinesis")>]
[<assembly: AssemblyVersionAttribute("0.4.0")>]
[<assembly: AssemblyFileVersionAttribute("0.4.0")>]
[<assembly: InternalsVisibleToAttribute("ReactoKinesiX.Tests")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.4.0"
