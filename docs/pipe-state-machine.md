# PipeCore State Machine

This page describes the runtime state transitions of the non-template scheduler core used by Pipe.
`PipeCore` is created only by successful `Pipe::initialize()`. The configuration
and cancellation phases live outside this diagram; see [Pipe Lifecycle](pipe-lifecycle.md).

## Core State Variables

- write_visible_pos: contiguous readable boundary reached by committed writes
- write_reserve_pos: logical end of reserved write region
- read_reserve_pos: logical end of reserved read region
- read_release_pos: logical end of fully released read region
- closed: whether the pipe is closed for new write acquisition

## Effective Runtime States

1. Open-Active
- closed is false
- producers can reserve the configured write size or an explicit count if free slots exist
- consumers can acquire read batches if readable data exists

2. Open-WriterBlocked
- closed is false
- producers are waiting because free slots are insufficient
- transition out when readers release consumed slots in order

3. Open-ReaderBlocked
- closed is false
- consumers are waiting because readable data is insufficient
- transition out when contiguous earlier write tokens are released and published

4. Closed-Draining
- closed is true
- no new write acquisition is allowed
- consumers continue draining readable data

5. Closed-And-Empty
- closed is true and readable data is exhausted
- acquire_read path throws PipeClosedAndEmptySignal

6. Write-Failure-Closed
- an earlier write token was released without publish
- later writes are not allowed to bypass the failed gap
- pipe transitions to closed terminal behavior
- future write acquisitions throw `PipeFaultedError`

## Transition Diagram

```mermaid
stateDiagram-v2
    [*] --> OpenActive

    OpenActive --> OpenWriterBlocked: writer acquire (no free slots)
    OpenActive --> OpenReaderBlocked: reader acquire (no readable data)
    OpenActive --> ClosedDraining: close()
    OpenActive --> WriteFailureClosed: earlier write token released without publish

    OpenWriterBlocked --> OpenActive: reader release frees slots
    OpenReaderBlocked --> OpenActive: earlier contiguous writes released and published

    OpenWriterBlocked --> ClosedDraining: close()
    OpenReaderBlocked --> ClosedDraining: close()
    OpenWriterBlocked --> WriteFailureClosed: earlier write token released without publish
    OpenReaderBlocked --> WriteFailureClosed: earlier write token released w/o publish

    ClosedDraining --> ClosedAndEmpty: readable drained
    WriteFailureClosed --> ClosedAndEmpty: visible readable data drained
    ClosedAndEmpty --> [*]
```

## Important Invariants

1. readers only observe a contiguous committed prefix of writes
2. later published writes never bypass earlier unresolved writes
3. when an earlier write token is destroyed without publish, the pipe enters a terminal failure-close path
4. read capacity is returned only when read tokens are released in order
5. close only prevents future write acquisition; it does not discard already visible readable data
6. the fault reason is retained as sticky metadata for diagnostics

## Mapping To Public API

- Pipe acquire_write reserves the configured write size or an explicit count through PipeCore
- Pipe publish marks reserved writes as committed; visibility advances only through contiguous release order
- Pipe acquire_read consumes the configured read window through PipeCore
- Initialized Pipe close forwards to PipeCore close; configuration-time close
  cancels preparation without constructing a core.
