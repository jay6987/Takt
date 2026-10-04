# Pipe Read Path Sequence

This page describes the read-side sequence, including overlap-window behavior.

## Sequence Diagram

```mermaid
sequenceDiagram
    participant Consumer
    participant Pipe as Pipe<T>
    participant Core as PipeCore

    Consumer->>Pipe: acquire_read() or acquire_read_batch(read_size, overlap_size)
    Pipe->>Pipe: snapshot previous read window (for overlap)
    Pipe->>Core: acquire_read_batch(read_size, overlap_size, has_prev, prev_size)

    alt enough readable data within current segment (or closed-and-draining)
        Core-->>Pipe: token id + slots + stride metadata
        Pipe-->>Pipe: compose overlap prefix + new slots
        Pipe-->>Consumer: ReadToken / ReadBatchToken
        Consumer->>Consumer: process values
        Consumer->>Pipe: token destructs
        Pipe->>Pipe: persist last_read window
        Pipe->>Core: release_read_batch(token_id)
        Core-->>Core: advance read_release_pos in token order
        Core-->>Core: notify readers/writers
    else closed and no contiguous readable data
        Core-->>Consumer: throw PipeClosedAndEmptySignal
    end
```

## Overlap Rules

- `overlap_size` must be smaller than `read_size`.
- Overlap read requires one previous completed read batch.
- Overlap prefix is sourced from the previous read window snapshot.
- Overlap is constrained to the same segment and never crosses a segment boundary.

## Segment Rules

- Read batches are clipped to the nearest committed segment end.
- `ReadToken`/`ReadBatchToken` expose `is_segment_start()` and `is_segment_end()`.
- Consumers can use segment flags to trigger end-of-shot flushing logic.

## Notes

- Read visibility is controlled by contiguous committed write order, not by isolated later publishes.
- Slot release occurs when read token lifetime ends, and free capacity is returned in read-token release order.
- If the pipe faulted because of an earlier unpublished write token, readers can only drain the already visible prefix, then receive `PipeClosedAndEmptySignal`.
- Core remains type-agnostic; overlap value composition is handled by Pipe<T>.
