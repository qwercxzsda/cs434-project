# Master Design

## Request Handler
### Register() : IpAddress
- Handle RegisterRequest, return the ip address of the worker

### Terminate() : Unit
- Handle TerminateRequest

## Main Class Method & Flow

### WaitForRegister() : List[IpAddress]
- Wait for n RegisterRequest, and return list of workers

### Sample(List[IpAddress]) : List[Data]
- Send SampleRequest to all worker node, and return the gathered sample data

### ComputeRange(List[Data], List[IpAddress]) : List[(Range, IpAddress)]
- Compute Range, Address pair

### InitiateSorting(List[(Range, IpAddress)]) : Unit
- Send InitiateRequest to all worker node

### WaitForTermination() : Unit
- Wait for n TerminateRequest