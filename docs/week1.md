# Week1

## Progress of this week

1. Set program goals
2. Configured basic communication sequence between master & worker nodes
3. Set repository management strategies

### Milestones
#### Milestone 0
- Design (State diagram, API specs, Data flows...)
- Coding convention
- Unit Test & System Test methods
- Configure error handling strategy


#### Milestone 1
- Generate input data
- Connect Master & Worker nodes
- Configure gRPC

#### Milestone 2
- Worker samples data -> give master its range
- Master computes ranges for each worker nodes given worker's range -> distribute it to workers with (range, ipaddress) list

#### Milestone 3
- Workers sort and partition its data (should decide the size of partitioned file)
- Workers distribute its files to appropriate node range.
- Workers merge data
- Workeres terminate & notify master

### Sequence Diagram
![image](https://github.com/qwercxzsda/cs434-project/assets/30927114/e39ac4d4-5b6f-41ba-a3eb-9dda0a5b1a3a)

### Repository Management Strategies
1. Feature -> Main branch
2. Only merge branches with pull request
3. Code review with github pull request
4. Smaller branches & frequent merge is preferred
5. Main branch should be consistent & have no compile error

## Goal of week 2
- Make state diagram & flow chart
- Set detailed design of each control step
- Study gRPC library
- Set detailed API specs for master & worker
- Set strategies for state & error management

** There is no seperate goal for each person this week.