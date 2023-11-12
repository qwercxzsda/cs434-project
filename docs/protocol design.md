# Protocol Design

## Master -> Worker
- `SampleRequest() : SampleResponse(List[Data])`
- `InitiateRequest(List[(Range, Address)]) : InitiateResponse()`

## Workler -> Master
- `RegisterRequest() : RegisterResponese()`
- `TerminateRequest() : TerminateResponse()`

## Worker -> Worker
- `SaveDataRequest(List[Data], Bool Finished) : SaveDataResponse()`