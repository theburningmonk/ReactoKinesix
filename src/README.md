Implementation Details
======================

#### Prerequisites

The implementation of the solution is heavily based on F#'s [MailboxProcessor<'T>](http://msdn.microsoft.com/en-us/library/ee370357.aspx) type and [Reactive Extensions](https://rx.codeplex.com/) (Rx) so it'll help your understanding of the implementation if you have at least basic understanding of the two, and not to mention some familiarity with F#!

If you are unfamiliar with *Rx* then a good place to start is the [Introduction to Rx](http://introtorx.com/) website, the [101 Rx samples](http://rxwiki.wikidot.com/101samples) wiki is also a good place to see plenty of *Rx* example usages.

The `MaiboxProcessor<'T>` type is F#'s implementation of the **Actor Model**, which nowadays can be found in many programming languages such as Scala, Haskell, Akka (for Java), etc. but most famously as `processes` in the fabulous language that is Erlang!

If you want to learn more about the *Actor Model*, I strongly recommend watching this [recorded conversation](http://channel9.msdn.com/Shows/Going+Deep/Hewitt-Meijer-and-Szyperski-The-Actor-Model-everything-you-wanted-to-know-but-were-afraid-to-ask) between **Carl Hewitt** (father of the *Actor Model*), **Erik Meijer** (of the *LINQ* and *Rx* fame) and *Clemens Szyperski*, with my summation of the key points [here](http://theburningmonk.com/2012/09/takeaways-from-hewitt-meijer-and-szyperskis-talk-on-the-actor-model/).

#### Project Structure

The project structure is simple as you'd expect with most F# projects, there's a total of 3 F# source files of interest:
- Model.fs
- Utils.fs
- Client.fs

**Model.fs** - this file contains the definition of all the public as well as internal models.

**Utils.fs** - this file contains helper functions for:
- communicating with `Amazon DynamoDB`
- communicating with `Amazon Kinesis`
- helpful extension methods

**Client.fs** - this file contains the implementation for:
- `ReactoKinesixApp` which represents a client application that consumes *records* from a *stream*
- `ReactoKinesixShardProcessor` which is responsible for managing the processing of a particular *shard* in the *stream*