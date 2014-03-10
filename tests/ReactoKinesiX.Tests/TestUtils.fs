namespace ReactoKinesix.Tests

open System.Collections.Generic
open System.Linq

type TestUtils =
    static member UnsafeInit<'a> () = System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof<'a>) :?> 'a

module Seq =
    let toResizeArray (seq : 'a seq) = new List<'a>(seq)

    let toDict (seq : ('a * 'b) seq) =
        let dict = new Dictionary<'a, 'b>()
        seq |> Seq.iter (fun (key, value) -> dict.Add(key, value))
        dict

    /// Try and take the specified number of items from the source sequence
    let tryTake limit (seq : 'a seq) = seq.Take limit