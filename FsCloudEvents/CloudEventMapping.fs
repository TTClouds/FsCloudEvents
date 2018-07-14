module FsCloudEvents.Mappings
open System
open Chessie.ErrorHandling
open System.Globalization

type ContextParser = (string * string) seq -> Result<CloudEventContext, string>
type ContextFormatter = CloudEventContext -> string seq

type CloudEventContextAttribute =
    | IgnoreAttribute
    | StandardAttribute of StandardContextAttributes * string
    | NonStandardAttribute of string * string
    | ExtendedAtttribute of string * string
    | ErrorAttribute of string

type AttributeMapping = string -> string -> CloudEventContextAttribute

let internal combineInternal k v current next =
    match current with
    | IgnoreAttribute -> next k v
    | x -> x

let combineTwo (m1: AttributeMapping) (m2: AttributeMapping) : AttributeMapping = 
    let mapping k v = combineInternal k v (m1 k v) m2
    mapping

let combine (ms: AttributeMapping seq) : AttributeMapping =
    let mapping k v =
        ms
        |> Seq.scan (combineInternal k v) IgnoreAttribute
        |> Seq.skipWhile (function IgnoreAttribute -> true | _ -> false)
        |> Seq.tryHead
        |> Option.defaultValue IgnoreAttribute
    mapping

type internal PartialCloudEventContext = {
    partialEventType: EventType option
    partialCloudEventsVersion: CloudEventsVersion option
    partialSource: Source option
    partialEventID: EventID option
    partialEventTime: EventTime option
    partialSchemaURL: SchemaURL option
    partialContentType: ContentType option
    partialExtensions: Extensions option
}

let internal emptyPartial = {
    partialEventType = None
    partialCloudEventsVersion = None
    partialSource = None
    partialEventID = None
    partialEventTime = None
    partialSchemaURL = None
    partialContentType = None
    partialExtensions = None
}

let internal addPartialExtension k v ctx =
    ctx.partialExtensions
        |> function Some x -> x | _ -> Map.empty
        |> Map.add k (cloudString v)
        |> Some
        |> fun ext -> { ctx with partialExtensions = ext }

let internal assignCheck 
    (ctx: PartialCloudEventContext) ws es name value
    (test: 'a option -> Result<unit, string>) 
    (get: PartialCloudEventContext -> 'a option) 
    (set: 'a option -> PartialCloudEventContext -> PartialCloudEventContext ) =
    match get ctx with 
    | Some _ -> ctx, ws, (List.append es [sprintf "Attribute %s is duplicated" name])
    | None ->
        match test (Some value) with
        | Ok ((), ws') -> (set (Some value) ctx), (List.append ws ws'), es
        | Bad es' -> ctx, ws, (List.append es es')

let internal assignStandardAttribute ctx ws es key value =
    match key with
    | StandardContextAttributes.EventType ->
        assignCheck ctx ws es "eventType" value Validate.eventType (fun c -> c.partialEventType) (fun v c -> { c with partialEventType = v })
    | StandardContextAttributes.CloudEventsVersion ->
        assignCheck ctx ws es "cloudEventsVersion" value Validate.cloudEventsVersion (fun c -> c.partialCloudEventsVersion) (fun v c -> { c with partialCloudEventsVersion = v })
    | StandardContextAttributes.Source ->
        assignCheck ctx ws es "source" value Validate.source (fun c -> c.partialSource) (fun v c -> { c with partialSource = v })
    | StandardContextAttributes.EventID ->
        assignCheck ctx ws es "eventID" value Validate.eventID (fun c -> c.partialEventID) (fun v c -> { c with partialEventID = v })
    | StandardContextAttributes.EventTime ->
        match ctx.partialEventTime with 
        | Some _ -> ctx, ws, (List.append es ["Attribute eventTime is duplicated"])
        | None ->
            let timeValueOk, timeValue =
                DateTimeOffset.TryParseExact(value, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
            if not timeValueOk then
                ctx, ws, (List.append es ["Invalid date-time format in attribute eventTime"])
            else
                match Validate.eventTime (Some timeValue) with
                | Ok ((), ws') -> 
                    { ctx with partialEventTime = Some timeValue }, (List.append ws ws'), es
                | Bad es' -> 
                    ctx, ws, (List.append es es')

        // assignCheck ctx ws es "eventTime" value Validate.eventTime (fun c -> c.partialEventTime) (fun v c -> { c with partialEventTime = v })
    | StandardContextAttributes.SchemaURL ->
        assignCheck ctx ws es "schemaURL" value Validate.schemaURL (fun c -> c.partialSchemaURL) (fun v c -> { c with partialSchemaURL = v })
    | StandardContextAttributes.ContentType ->
        assignCheck ctx ws es "contentType" value Validate.contentType (fun c -> c.partialContentType) (fun v c -> { c with partialContentType = v })
    | _ ->
        ctx, ws, List.append es ["Unknown standard context attribute."]

let internal parsePartialContext mappings pairs =
    let folder (ctx, ws, es) (k, v) =
        match mappings k v with
        | IgnoreAttribute ->
            (ctx, ws, es)
        | ErrorAttribute error ->
            (ctx, ws, error :: es)
        | ExtendedAtttribute (key, value) ->
            let ctx' = ctx |> addPartialExtension key value
            ctx', ws, es
        | NonStandardAttribute (key, value) ->
            (ctx, ws, ("Unknown context attribute. Use it as an extension") :: es)
        | StandardAttribute (key, value) ->
            assignStandardAttribute ctx ws es key value
    pairs 
        |> Seq.fold folder (emptyPartial, [], [])

// let parseContext (mappings: AttributeMapping) : ContextParser =
//     let parser pairs =
//         use e = (pairs: _ seq).GetEnumerator()
//         let rec loop ev moved = 
//             ev
//         let partial = loop (ok emptyPartial) (e.MoveNext())

//     parser

