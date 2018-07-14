namespace FsCloudEvents
open System
open System.Text.RegularExpressions
open Chessie.ErrorHandling

// https://github.com/cloudevents/spec/blob/master/spec.md#type-system
type CeString = string
and  CeBinary = ArraySegment<byte>
and  CeMap = Map<CeString, CeObject>
and  CeObject = 
    | CloudString of CeString
    | CloudBinary of CeBinary
    | CloudMap of CeMap
and  CeUri = CeString
and  CeTimestamp = DateTimeOffset

[<AutoOpen>]
module CeValues =
    let cloudString = CloudString
    let cloudBinary = CloudBinary
    let cloudArray arr = ArraySegment<byte>(arr) |> cloudBinary
    let cloudMap = CloudMap
    let mapFromStrings (kvs: (string * string) seq): CeMap =
        kvs
        |> Seq.map (fun (k, v) -> (k, cloudString v))
        |> Map.ofSeq

// https://github.com/cloudevents/spec/blob/master/spec.md#context-attributes
type EventType = CeString
type CloudEventsVersion = CeString
type Source = CeUri
type EventID = CeString
type EventTime = DateTimeOffset
type SchemaURL = CeUri
type ContentType = CeString
type Extensions = CeMap

type StandardContextAttributes =
    | EventType = 1
    | CloudEventsVersion = 2
    | Source = 3
    | EventID = 4
    | EventTime = 5
    | SchemaURL = 6
    | ContentType = 7

type CloudEventContext = {
    eventType: EventType
    cloudEventsVersion: CloudEventsVersion
    source: Source
    eventID: EventID
    eventTime: EventTime option
    schemaURL: SchemaURL option
    contentType: ContentType option
    extensions: Extensions option
}

[<RequireQualifiedAccessAttribute>]
module Validate =

    let isRequiredOption name value =
        match value with
        | Some _ -> ok()
        | None -> 
            fail <| sprintf "%s is REQUIRED" name

    let mustBeANonEmptyString name value =
        if String.IsNullOrEmpty value then 
            fail <| sprintf "%s MUST be a non-empty string" name
        else
            ok()

    let private reverseDnsRegexp = Regex("^\w+(\.\w+)+$", RegexOptions.Compiled)
    let shouldBeReverseDNS name value =
        if reverseDnsRegexp.IsMatch value then 
            ok()
        else
            warn (sprintf "%s SHOULD be prefixed with a reverse-DNS name" name) ()

    let private mediaTypeRegexp = Regex("^\w+\/[\w\.\+\-]+$", RegexOptions.Compiled)
    let mustBeValidMediaType name value =
        if mediaTypeRegexp.IsMatch value then 
            ok()
        else
            fail (sprintf "%s MUST adhere to the format specified in RFC 2046" name)

    let mustBeValidUri name value =
        if Uri.IsWellFormedUriString(value, UriKind.RelativeOrAbsolute) then 
            ok()
        else
            warn (sprintf "%s MUST be a valid URI" name) ()

    let mustContainAtLeastOneEntry name value =
        if Map.count value > 0 then 
            ok()
        else
            warn (sprintf "%s MUST contain at least one entry" name) ()

    let optional test name value =
        match value with
        | None -> ok()
        | Some v -> test name v

    let ifPresentOption checker name value =
        match value with
        | None -> ok()
        | Some value -> 
            let mapper = List.map (sprintf "If present, %s")
            match checker name value with
            | Ok((), ws) -> Ok((), mapper ws)
            | Bad es -> Bad <| mapper es

    let combineTests tests name value = 
        let rec loop list result =
            match result, list with
            | Bad _, _ -> result
            | _, [] -> result
            | Ok ((), ws), test :: list' ->
                let result' = 
                    match test name value with
                    | Ok ((), ws') -> Ok((), List.append ws ws')
                    | Bad es' -> Bad es'
                loop list' result'
        loop tests <| ok()

    let eventType x = 
        combineTests [
            isRequiredOption
            optional mustBeANonEmptyString
            optional shouldBeReverseDNS
        ] "eventType" x

    let cloudEventsVersion = 
        combineTests [
            isRequiredOption
            optional mustBeANonEmptyString
        ] "cloudEventsVersion"

    let source = 
        combineTests [
            isRequiredOption
            optional mustBeANonEmptyString
            optional mustBeValidUri
        ] "source"

    let eventID = 
        combineTests [
            isRequiredOption
            optional mustBeANonEmptyString
        ] "eventID"

    let schemaURL = 
        combineTests [
            ifPresentOption mustBeANonEmptyString
            ifPresentOption mustBeValidUri
        ] "schemaURL"

    let contentType = 
        combineTests [
            ifPresentOption mustBeValidMediaType
        ] "contentType"

    let eventTime (_: EventTime option): Result<unit, string> = ok ()

    let extensions e = 
        ifPresentOption mustContainAtLeastOneEntry "extensions" e

module CloudEventContext =
    let empty = {
        eventType = ""
        cloudEventsVersion = "0.1"
        source = ""
        eventID = ""
        eventTime = None
        schemaURL = None
        contentType = None
        extensions = None
    }

    let getEventType ctx = ctx.eventType
    let setEventType e ctx = { ctx with eventType = e }

    let getCloudEventsVersion ctx = ctx.cloudEventsVersion
    let setCloudEventsVersion e ctx = { ctx with cloudEventsVersion = e }

    let getSource ctx = ctx.source
    let setSource e ctx = { ctx with source = e }

    let getEventID ctx = ctx.eventID
    let setEventID e ctx = { ctx with eventID = e }

    let getEventTime ctx = ctx.eventTime
    let setEventTime e ctx = { ctx with eventTime = e }

    let getSchemaURL ctx = ctx.schemaURL
    let setSchemaURL e ctx = { ctx with schemaURL = e }

    let getContentType ctx = ctx.contentType
    let setContentType e ctx = { ctx with contentType = e }

    let getExtensions ctx = ctx.extensions
    let setExtensions e ctx = { ctx with extensions = e }

    let addExtension k v ctx =
        ctx
        |> getExtensions
        |> function Some x -> x | _ -> Map.empty
        |> Map.add k (cloudString v)
        |> Some
        |> fun ext -> setExtensions ext ctx
    let removeExtensions k ctx =
        ctx
        |> getExtensions
        |> function Some x -> x | _ -> Map.empty
        |> Map.remove k
        |> fun m -> if Map.isEmpty m then None else Some m
        |> fun ext -> setExtensions ext ctx

type CloudEvent =
    | EmptyCloudEvent of CloudEventContext
    | ObjectCloudEvent of CloudEventContext * obj
    | StringCloudEvent of CloudEventContext * string
    | BinaryCloudEvent of CloudEventContext * byte[]

module CloudEvent =
    let empty = EmptyCloudEvent

    let newObjectEvent value ctx = ObjectCloudEvent(ctx, value)
    let newStringEvent str ctx = StringCloudEvent(ctx, str)
    let newBinaryEvent bytes ctx = BinaryCloudEvent(ctx, bytes)

    let getContext = function
        | EmptyCloudEvent c -> c
        | ObjectCloudEvent (c, _) -> c
        | StringCloudEvent (c, _) -> c
        | BinaryCloudEvent (c, _) -> c

    let setContext ev ctx =
        match ev with
        | EmptyCloudEvent _ -> EmptyCloudEvent ctx
        | ObjectCloudEvent (_, x) -> ObjectCloudEvent (ctx, x)
        | StringCloudEvent (_, x) -> StringCloudEvent (ctx, x)
        | BinaryCloudEvent (_, x) -> BinaryCloudEvent (ctx, x)

    let updateContextFn f ev = ev |> getContext |> f |> setContext ev
    let updateContext newCtx = updateContextFn <| fun _ -> newCtx

    let toEmpty ev = ev |> getContext |> empty
    let toObjectEvent o = getContext >> newObjectEvent o
    let toStringEvent o = getContext >> newStringEvent o
    let toBinaryEvent o = getContext >> newBinaryEvent o

    let getObjectPayload = function | ObjectCloudEvent (_, o) -> Some o | _ -> None
    let getStringPayload = function | StringCloudEvent (_, o) -> Some o | _ -> None
    let getBinaryPayload = function | BinaryCloudEvent (_, o) -> Some o | _ -> None
