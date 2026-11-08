# Pipe Write Path Sequence

This page describes the write-side sequence from producer code to PipeCore state updates.

## Sequence Diagram

```mermaid
sequenceDiagram
    participant Producer
    participant Pipe as Pipe<T>
    participant Core as PipeCore

    Producer->>Pipe: acquire_write() or acquire_shot_end_write(count)
    Pipe->>Core: reserve configured default or explicit count

    alt free slots available and not closed
        Core-->>Pipe: slots reserved
        Pipe-->>Producer: WriteToken / WriteReservation
        Producer->>Pipe: write values into frames
        Producer->>Pipe: publish()
        Note over Producer,Pipe: data committed, token still owns reserved region
        Producer->>Pipe: token destructs / releases
        Pipe->>Core: release_write_reservation(token_id, published=true)
        Core-->>Core: advance contiguous visible write boundary in token order
        Core-->>Core: notify readers/writers
    else closed or no space
        Core-->>Producer: block or throw PipeClosedAndEmptySignal
    end

    Note over Producer,Core: No-argument writes reserve frames_per_write and are not ShotEnd; acquire_shot_end_write explicitly marks final writes

    opt close with an unfinished Shot
        Producer->>Pipe: close()
        Pipe->>Core: close and notify waiters
        Core-->>Producer: throw PipeUnterminatedShotError
        Note over Pipe,Core: Published tail remains drainable without a fabricated ShotEnd
    end

    opt token destroyed without publish
        Pipe->>Core: release_write_reservation(count, published=false)
        Core-->>Core: mark sticky fault on earliest unresolved write gap
        Core-->>Core: close future write acquisition
        Core-->>Core: future acquire_write throws PipeFaultedError
    end
```

## Notes

- Capacity reservation happens before producer writes element payload.
- Each write token carries Shot metadata: `is_shot_start` and `is_shot_end`.
- Shot boundaries are committed in write-token order with the same visibility rules as payload data.
- `publish()` does not by itself make data visible to readers.
- Visibility advances when a published write token is released, and only if all earlier write tokens have already been released and published.
- A published token that remains alive still occupies capacity and can stall downstream visibility.
- Destroying a write token without publish triggers sticky pipe fault semantics.
- Backpressure is enforced by Core through available-slot checks.
