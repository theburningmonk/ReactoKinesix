[![Issue Stats](http://issuestats.com/github/theburningmonk/ReactoKinesix/badge/issue)](http://issuestats.com/github/theburningmonk/ReactoKinesix)
[![Issue Stats](http://issuestats.com/github/theburningmonk/ReactoKinesix/badge/pr)](http://issuestats.com/github/theburningmonk/ReactoKinesix)
[![Travis build status](https://travis-ci.org/theburningmonk/ReactoKinesiX.png)](https://travis-ci.org/theburningmonk/ReactoKinesiX)
[![AppVeyor Build status](https://ci.appveyor.com/api/projects/status/em45jjc8u2rnm88g/branch/develop)](https://ci.appveyor.com/project/theburningmonk/ReactoKinesix/branch/develop)
[![NuGet Status](http://img.shields.io/nuget/v/Paket.svg?style=flat)](https://www.nuget.org/packages/reactokinesix/)

Reacto-Kinesix
=======================

A [Rx](https://rx.codeplex.com/)-based .Net client library for [Amazon Kinesis](http://aws.amazon.com/kinesis/).

This client library makes it easy for you to build a record-consuming real time applications on top of the Amazon Kinesis service.

It takes care of the plumbing required to track your progress and manage sharding changes in the underlying stream, as well as giving you different options to handle errors on a record-by-record basis.

Scaling out your application is also supported and handled by the library, as new nodes start the processing of shards will be spread and load balanced automatically.


## Online resources

* [Documentation][docs]
* Download [Nuget package][nuget]

## Troubleshooting and support

* Found a bug or missing a feature? Feed the [issue tracker][issues].
* Announcements and related news through Twitter ([@Twitter][twitter]).

## License

The [MIT license][license]

## Maintainer(s)

* [@theburningmonk](https://github.com/theburningmonk)


[docs]: http://theburningmonk.github.io/ReactoKinesix
[nuget]: https://www.nuget.org/packages/ReactoKinesix
[twitter]: https://twitter.com/ReactoKinesix
[license]: https://github.com/theburningmonk/ReactoKinesix/blob/develop/LICENSE.txt
[issues]: https://github.com/theburningmonk/ReactoKinesix/issues