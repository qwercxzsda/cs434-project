# Worker Design

## Master Request Handler

### Sample() : Unit
- Handle SampleRequest, send sampled data as a response

### Initiate() : List[(Range, IpAddress)]
- Handle InitiateSortingrequest, returns the worker ranges given by master

## Worker Request Handler

### SaveData() : (IpAddress, bool Finished)
- Handle SaveDataRequest

## Main Class Method & Flow

### Register(IpAddress, Port) : Unit
- Send RegisterRequest to given Address, Port

### Sample() : Unit
- Wait for SampleRequest, and send sampled data as a response

### Initiate() : List[(Range, IpAddress)]
- Wait for InitiateSortingrequest, and return range, address pairs.

### Sort() : Unit
- Sort local data

### Partition(List[(Range, IpAddress)]) : Unit
- Partition local data with ranges

### SendData(List[(Range, IpAddress)]) : Unit
- Send data to appropriate worker node. If the data send for a node is finished, send SaveDataRequest with Finished=true

### Terminate(IpAddress, Port) : Unit
- Send TerminateRequest to master

