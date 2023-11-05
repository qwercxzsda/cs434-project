# Week2

## Progress of this week

1. Set detailed data flow of the program.
2. Separate modules 
3. Set error management strategy

### Data Flow Diagram
![IMG_A0D57497DBC8-1](https://github.com/qwercxzsda/cs434-project/assets/30927114/3dd0d23e-5f37-4fdd-9766-a98ee30d072d)

### Module Design
![IMG_37688CB82A17-1](https://github.com/qwercxzsda/cs434-project/assets/30927114/d9c9237b-a873-46c3-b440-f5b3d6f0995b)

### Error Management Strategy
- Master Error: Do not handle error
- Worker Error: If there is an error in Sort & Partition, Distribute, Merge Phase, notify master. Else, just throw an error and terminate.

## Goal of week 3
- Set detailed design of each control step & api specs
- Study gRPC library
- Jeonho: Connect Master & Worker nodes
- Jihun: Generate input data