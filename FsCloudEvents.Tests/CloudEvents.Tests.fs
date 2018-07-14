namespace FsCloudEvents.Tests

open System
open FSharp.Core
open NUnit.Framework
open FsUnit
open FsCloudEvents
open Chessie.ErrorHandling

module ``CloudEventContext Tests`` =
    let ctx1: CloudEventContext = {
        eventType = "com.github.pull.create"
        cloudEventsVersion = "0.1"
        source = "https://producer.example.com"
        eventID = "123456"
        eventTime = Some (DateTimeOffset(0L, TimeSpan.Zero))
        schemaURL = Some "https://github.com/ttclouds/events/pull.create"
        contentType = Some "application/octet-stream"
        extensions = Some Map.empty
    }

    [<Test>]
    let ``CloudEventContext.getEventType``() =
        ctx1 
        |> CloudEventContext.getEventType 
        |> should equal ctx1.eventType

    [<Test>]
    let ``CloudEventContext.setEventType``() =
        ctx1 
        |> CloudEventContext.setEventType "new-value"
        |> CloudEventContext.getEventType 
        |> should equal "new-value"

    [<Test>]
    let ``CloudEventContext.getCloudEventsVersion``() =
        ctx1 
        |> CloudEventContext.getCloudEventsVersion 
        |> should equal ctx1.cloudEventsVersion

    [<Test>]
    let ``CloudEventContext.setCloudEventsVersion``() =
        ctx1 
        |> CloudEventContext.setCloudEventsVersion "new-value"
        |> CloudEventContext.getCloudEventsVersion 
        |> should equal "new-value"

    [<Test>]
    let ``CloudEventContext.getSource``() =
        ctx1 
        |> CloudEventContext.getSource 
        |> should equal ctx1.source

    [<Test>]
    let ``CloudEventContext.setSource``() =
        ctx1 
        |> CloudEventContext.setSource "new-value"
        |> CloudEventContext.getSource 
        |> should equal "new-value"

    [<Test>]
    let ``CloudEventContext.getEventID``() =
        ctx1 
        |> CloudEventContext.getEventID 
        |> should equal ctx1.eventID

    [<Test>]
    let ``CloudEventContext.setEventID``() =
        ctx1 
        |> CloudEventContext.setEventID "new-value"
        |> CloudEventContext.getEventID 
        |> should equal "new-value"

    [<Test>]
    let ``CloudEventContext.getEventTime``() =
        ctx1 
        |> CloudEventContext.getEventTime 
        |> should equal ctx1.eventTime

    [<Test>]
    let ``CloudEventContext.setEventTime``() =
        let newTime = DateTimeOffset(1000000000L, TimeSpan.Zero)
        ctx1 
        |> CloudEventContext.setEventTime (Some newTime)
        |> CloudEventContext.getEventTime 
        |> should equal (Some newTime)

    [<Test>]
    let ``CloudEventContext.getSchemaURL``() =
        ctx1 
        |> CloudEventContext.getSchemaURL 
        |> should equal ctx1.schemaURL

    [<Test>]
    let ``CloudEventContext.setSchemaURL``() =
        ctx1 
        |> CloudEventContext.setSchemaURL (Some "new-value")
        |> CloudEventContext.getSchemaURL 
        |> should equal (Some "new-value")

    [<Test>]
    let ``CloudEventContext.getContentType``() =
        ctx1 
        |> CloudEventContext.getContentType 
        |> should equal ctx1.contentType

    [<Test>]
    let ``CloudEventContext.setContentType``() =
        ctx1 
        |> CloudEventContext.setContentType (Some "new-value")
        |> CloudEventContext.getContentType 
        |> should equal (Some "new-value")

    [<Test>]
    let ``CloudEventContext.getExtensions``() =
        ctx1 
        |> CloudEventContext.getExtensions 
        |> should equal ctx1.extensions

    [<Test>]
    let ``CloudEventContext.setExtensions``() =
        let newExtensions = 
            [
                "key1", "value1"
                "key2", "value2"
            ]
            |> mapFromStrings
        ctx1 
        |> CloudEventContext.setExtensions (Some newExtensions)
        |> CloudEventContext.getExtensions 
        |> should equal (Some newExtensions)


module ``CloudEventContext Validate Tests`` =

    [<Test>]
    let ``Validate.eventType None``() =
        let expected : Result<unit, _> = Bad ["eventType is REQUIRED"]
        None |> Validate.eventType |> should equal expected

    [<Test>]
    let ``Validate.eventType Some ""``() =
        let expected : Result<unit, _> = Bad ["eventType MUST be a non-empty string"]
        Some "" |> Validate.eventType |> should equal expected

    [<Test>]
    let ``Validate.eventType Some "???"``() =
        let expected : Result<unit, _> = Ok((), ["eventType SHOULD be prefixed with a reverse-DNS name"])
        Some "???" |> Validate.eventType |> should equal expected

    [<Test>]
    let ``Validate.cloudEventsVersion None``() =
        let expected : Result<unit, _> = Bad ["cloudEventsVersion is REQUIRED"]
        None |> Validate.cloudEventsVersion |> should equal expected

    [<Test>]
    let ``Validate.cloudEventsVersion Some ""``() =
        let expected : Result<unit, _> = Bad ["cloudEventsVersion MUST be a non-empty string"]
        Some "" |> Validate.cloudEventsVersion |> should equal expected

    [<Test>]
    let ``Validate.cloudEventsVersion Some "0.1"``() =
        let expected : Result<unit, string> = ok()
        Some "0.1" |> Validate.cloudEventsVersion |> should equal expected

    [<Test>]
    let ``Validate.source None``() =
        let expected : Result<unit, _> = Bad ["source is REQUIRED"]
        None |> Validate.source |> should equal expected

    [<Test>]
    let ``Validate.source Some ""``() =
        let expected : Result<unit, _> = Bad ["source MUST be a non-empty string"]
        Some "" |> Validate.source |> should equal expected

    [<Test>]
    let ``Validate.source Some "not a valid uri"``() =
        let expected : Result<unit, _> = Ok((), ["source MUST be a valid URI"])
        Some "not a valid uri" |> Validate.source |> should equal expected

    [<Test>]
    let ``Validate.source Some "http://example.com/my/uri"``() =
        let expected : Result<unit, string> = ok()
        Some "http://example.com/my/uri" |> Validate.source |> should equal expected

    [<Test>]
    let ``Validate.eventID None``() =
        let expected : Result<unit, _> = Bad ["eventID is REQUIRED"]
        None |> Validate.eventID |> should equal expected

    [<Test>]
    let ``Validate.eventID Some ""``() =
        let expected : Result<unit, _> = Bad ["eventID MUST be a non-empty string"]
        Some "" |> Validate.eventID |> should equal expected

    [<Test>]
    let ``Validate.eventID Some "1234ABCD"``() =
        let expected : Result<unit, string> = ok()
        Some "1234ABCD" |> Validate.eventID |> should equal expected

    [<Test>]
    let ``Validate.schemaURL None``() =
        let expected : Result<unit, string> = ok()
        None |> Validate.schemaURL |> should equal expected

    [<Test>]
    let ``Validate.schemaURL Some ""``() =
        let expected : Result<unit, _> = Bad ["If present, schemaURL MUST be a non-empty string"]
        Some "" |> Validate.schemaURL |> should equal expected

    [<Test>]
    let ``Validate.schemaURL Some "not a valid uri"``() =
        let expected : Result<unit, string> = warn "If present, schemaURL MUST be a valid URI" ()
        Some "not a valid uri" |> Validate.schemaURL |> should equal expected

    [<Test>]
    let ``Validate.schemaURL Some "http://example.com/my/uri"``() =
        let expected : Result<unit, string> = ok()
        Some "http://example.com/my/uri" |> Validate.schemaURL |> should equal expected

    [<Test>]
    let ``Validate.contentType None``() =
        let expected : Result<unit, string> = ok()
        None |> Validate.contentType |> should equal expected

    [<Test>]
    let ``Validate.contentType Some ""``() =
        let expected : Result<unit, _> = Bad ["If present, contentType MUST adhere to the format specified in RFC 2046"]
        Some "" |> Validate.contentType |> should equal expected

    [<Test>]
    let ``Validate.contentType Some "not a valid media type"``() =
        let expected : Result<unit, _> = Bad ["If present, contentType MUST adhere to the format specified in RFC 2046"]
        Some "not a valid media type" |> Validate.contentType |> should equal expected

    [<Test>]
    let ``Validate.contentType Some "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"``() =
        let expected : Result<unit, string> = ok()
        Some "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" |> Validate.contentType |> should equal expected


module ``CloudEvent Tests`` =
    let ctx1: CloudEventContext = {
        eventType = "com.github.pull.create"
        cloudEventsVersion = "0.1"
        source = "https://producer.example.com"
        eventID = "123456"
        eventTime = Some (DateTimeOffset(0L, TimeSpan.Zero))
        schemaURL = Some "https://github.com/ttclouds/events/pull.create"
        contentType = Some "application/octet-stream"
        extensions = Some Map.empty
    }

    [<Test>]
    let ``CloudEvent.empty``() =
        CloudEvent.empty ctx1
        |> should equal (EmptyCloudEvent ctx1)

    [<Test>]
    let ``CloudEvent.newObjectEvent``() =
        let testObj = obj()
        CloudEvent.newObjectEvent testObj ctx1
        |> should equal (ObjectCloudEvent (ctx1, testObj))

    [<Test>]
    let ``CloudEvent.newStringEvent``() =
        let testObj = "hello world"
        CloudEvent.newStringEvent testObj ctx1
        |> should equal (StringCloudEvent (ctx1, testObj))

    [<Test>]
    let ``CloudEvent.newBinaryEvent``() =
        let testObj = [| 1uy; 2uy; 3uy |]
        CloudEvent.newBinaryEvent testObj ctx1
        |> should equal (BinaryCloudEvent (ctx1, testObj))

    [<Test>]
    let ``CloudEvent.updateContextFn``() =
        let ctxFn = CloudEventContext.setEventID "abcd"
        CloudEvent.empty ctx1
        |> CloudEvent.updateContextFn ctxFn
        |> should equal (CloudEvent.empty <| ctxFn ctx1)

    [<Test>]
    let ``CloudEvent.updateContext``() =
        let ctx2 = ctx1 |> CloudEventContext.setEventID "abcd"
        CloudEvent.empty ctx1
        |> CloudEvent.updateContext ctx2
        |> should equal (CloudEvent.empty ctx2)
