# Protocol Design

## Master -> Worker
- `RegisterCompleteRequest(Map(Address, List(Record))) : RegisterResponese()`
- `SortStartRequest() : SortStartResponse()`
## Worker -> Master
- `RegisterRequest(List(Record)) : RegisterResponese()`
- `DistributeCompleteRequest(Address) : DistributeCompleteResponse`
- `SortCompleteRequest(Address) : SortCompleteResponse()`


## Worker -> Worker
- `SendDataRequest(List[Data]) : SaveDataResponse()`
