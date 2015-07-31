(**
# What is ReactoKinesix?

It's a [Rx](https://rx.codeplex.com/)-based .Net client library for [Amazon Kinesis](http://aws.amazon.com/kinesis/).

This client library makes it easy for you to build a record-consuming real time applications on top of the Amazon Kinesis service.

It takes care of the plumbing required to track your progress and manage sharding changes in the underlying stream, as well as giving you different options to handle errors on a record-by-record basis.

Scaling out your application is also supported and handled by the library, as new nodes start the processing of shards will be spread and load balanced automatically.

This guide contains the following sections:

- [Getting Started](getting-started.html)
- [Features](features.html)
- [Error Handling](error-handling.html)
- [Distributed Processing](distributed-processing.html)

You can report issues [here](https://github.com/theburningmonk/ReactoKinesiX/issues).

Samples & documentation
-----------------------

The library comes with comprehensible documentation. 
It can include tutorials automatically generated from `*.fsx` files in [the content folder][content]. 
The API reference is automatically generated from Markdown comments in the library implementation.

 * [C# example](example-csharp.html) contains a C# sample.

 * [F# example](example-fsharp.html) contains a F# sample.

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding a new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read the [library design notes][readme] to understand how it works.

The library is available under Public Domain license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/theburningmonk/ReactoKinesix/tree/master/docs/content
  [gh]: https://github.com/theburningmonk/ReactoKinesix
  [issues]: https://github.com/theburningmonk/ReactoKinesix/issues
  [readme]: https://github.com/theburningmonk/ReactoKinesix/blob/master/README.md
  [license]: https://github.com/theburningmonk/ReactoKinesix/blob/master/LICENSE.txt
*)
