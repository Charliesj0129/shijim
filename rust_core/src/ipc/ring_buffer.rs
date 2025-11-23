use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Slot payload size in bytes. Must match the shared schema used by readers.
pub const SLOT_SIZE: usize = 48;

#[derive(Debug, thiserror::Error)]
pub enum RingError {
    #[error("packet too large: {got} > {max}")]
    PacketTooLarge { got: usize, max: usize },
    #[error("capacity must be power of two")]
    CapacityNotPowerOfTwo,
    #[error("batch larger than capacity: {batch}")]
    BatchOverflow { batch: u64 },
}

#[repr(C, align(64))]
pub struct RingBufferHeader {
    write_cursor: AtomicU64,
    _pad: [u8; 56],
}

impl RingBufferHeader {
    #[inline]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.write_cursor.load(ordering)
    }

    #[inline]
    pub fn store(&self, val: u64, ordering: Ordering) {
        self.write_cursor.store(val, ordering);
    }
}

/// Single-writer view of a ring buffer living in shared memory.
///
/// Safety invariants:
/// - Only one writer may hold this struct.
/// - The backing memory must be laid out as [RingBufferHeader][padding]+[slots...].
pub struct RingWriter {
    header: *const RingBufferHeader,
    buf: *mut u8,
    capacity: usize,
    mask: usize,
}

impl RingWriter {
    /// # Safety
    /// `base` must point to a contiguous region with:
    /// - header at offset 0 (64 bytes, repr(C, align(64)))
    /// - followed by `capacity * SLOT_SIZE` bytes for slots.
    /// Caller must enforce single-writer discipline.
    pub unsafe fn new(base: *mut u8, capacity: usize) -> Result<Self, RingError> {
        if !capacity.is_power_of_two() {
            return Err(RingError::CapacityNotPowerOfTwo);
        }
        let header = base as *const RingBufferHeader;
        let buf = base.add(std::mem::size_of::<RingBufferHeader>());
        Ok(Self {
            header,
            buf,
            capacity,
            mask: capacity - 1,
        })
    }

    #[inline]
    fn load_cursor(&self) -> u64 {
        // Acquire to see committed payloads from the previous Release store.
        unsafe { (*self.header).load(Ordering::Acquire) }
    }

    #[inline]
    fn slot_ptr(&self, idx: usize) -> *mut u8 {
        unsafe { self.buf.add(idx * SLOT_SIZE) }
    }

    #[inline]
    fn check_packet_size(&self, len: usize) -> Result<(), RingError> {
        if len > SLOT_SIZE {
            Err(RingError::PacketTooLarge {
                got: len,
                max: SLOT_SIZE,
            })
        } else {
            Ok(())
        }
    }

    /// Publish a single packet.
    pub fn publish(&self, packet: &[u8]) -> Result<u64, RingError> {
        self.check_packet_size(packet.len())?;
        let cursor = self.load_cursor();
        let idx = (cursor as usize) & self.mask;
        unsafe {
            ptr::copy_nonoverlapping(packet.as_ptr(), self.slot_ptr(idx), packet.len());
            if packet.len() < SLOT_SIZE {
                ptr::write_bytes(self.slot_ptr(idx).add(packet.len()), 0, SLOT_SIZE - packet.len());
            }
            // Release so readers see the payload before cursor is visible.
            (*self.header).store(cursor.wrapping_add(1), Ordering::Release);
        }
        Ok(cursor + 1)
    }

    /// Reserve a range for batch writing. Returns the start cursor.
    pub fn reserve(&self, batch: u64) -> Result<u64, RingError> {
        if batch as usize > self.capacity {
            return Err(RingError::BatchOverflow { batch });
        }
        Ok(self.load_cursor())
    }

    /// Commit a previously reserved batch. Updates cursor once with Release.
    pub fn commit(&self, start: u64, count: u64) -> Result<u64, RingError> {
        if count as usize > self.capacity {
            return Err(RingError::BatchOverflow { batch: count });
        }
        unsafe {
            (*self.header).store(start.wrapping_add(count), Ordering::Release);
        }
        Ok(start + count)
    }

    /// Write a batch of packets starting at `start` and commit them.
    pub fn write_batch(&self, start: u64, packets: &[impl AsRef<[u8]>]) -> Result<u64, RingError> {
        let count = packets.len() as u64;
        if count as usize > self.capacity {
            return Err(RingError::BatchOverflow { batch: count });
        }
        for (i, pkt) in packets.iter().enumerate() {
            let cursor = start + i as u64;
            let idx = (cursor as usize) & self.mask;
            let p = pkt.as_ref();
            self.check_packet_size(p.len())?;
            unsafe {
                ptr::copy_nonoverlapping(p.as_ptr(), self.slot_ptr(idx), p.len());
                if p.len() < SLOT_SIZE {
                    ptr::write_bytes(self.slot_ptr(idx).add(p.len()), 0, SLOT_SIZE - p.len());
                }
            }
        }
        self.commit(start, count)
    }

    /// Compute lag and signal if slow consumer exceeds capacity.
    pub fn slow_consumer_alert(&self, slowest_cursor: u64) -> bool {
        let write_cursor = self.load_cursor();
        let lag = write_cursor.saturating_sub(slowest_cursor);
        lag as usize > self.capacity
    }
}
