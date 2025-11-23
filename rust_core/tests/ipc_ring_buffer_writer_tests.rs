use rust_core::ipc::ring_buffer::{RingBufferHeader, RingError, RingWriter, SLOT_SIZE};
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

fn make_buffer(cap: usize) -> Vec<u8> {
    // header(64) + capacity * SLOT_SIZE
    vec![0u8; mem::size_of::<RingBufferHeader>() + cap * SLOT_SIZE]
}

#[test]
fn publish_wraps_and_updates_cursor() {
    let cap = 1024;
    let mut backing = make_buffer(cap);
    let base = backing.as_mut_ptr();
    unsafe {
        let header = base as *mut RingBufferHeader;
        (*header).store(1023, Ordering::Relaxed);
        let writer = RingWriter::new(base, cap).unwrap();
        let pkt = [0xAAu8; SLOT_SIZE];
        writer.publish(&pkt).unwrap(); // writes slot 1023 -> cursor 1024
        writer.publish(&pkt).unwrap(); // wraps to 0 -> cursor 1025
        let cursor = (*header).load(Ordering::Acquire);
        assert_eq!(cursor, 1025);
        assert_eq!(backing[mem::size_of::<RingBufferHeader>()], 0xAA); // first byte of slot 0
    }
}

#[test]
fn batch_commit_updates_cursor_once() {
    let cap = 1024;
    let mut backing = make_buffer(cap);
    let base = backing.as_mut_ptr();
    unsafe {
        let header = base as *mut RingBufferHeader;
        let writer = RingWriter::new(base, cap).unwrap();
        let start = writer.reserve(5).unwrap();
        assert_eq!(start, 0);
        let pkts: Vec<Vec<u8>> = (0..5).map(|i| vec![i as u8; SLOT_SIZE]).collect();
        let end = writer.write_batch(start, &pkts).unwrap();
        assert_eq!(end, 5);
        let cursor = (*header).load(Ordering::Acquire);
        assert_eq!(cursor, 5);
        assert_eq!(backing[mem::size_of::<RingBufferHeader>()], 0); // slot 0 first byte
        let slot4_offset = mem::size_of::<RingBufferHeader>() + 4 * SLOT_SIZE;
        assert_eq!(backing[slot4_offset], 4); // slot 4 first byte
    }
}

#[test]
fn layout_padding_cacheline() {
    assert_eq!(64, mem::size_of::<RingBufferHeader>());
    assert_eq!(0, mem::size_of::<RingBufferHeader>() % 64);
}

#[test]
fn slow_consumer_alert_when_lag_exceeds_capacity() {
    let cap = 1024;
    let mut backing = make_buffer(cap);
    let base = backing.as_mut_ptr();
    unsafe {
        let header = base as *mut RingBufferHeader;
        (*header).store(2000, Ordering::Release);
        let writer = RingWriter::new(base, cap).unwrap();
        assert!(writer.slow_consumer_alert(500));
        assert!(!writer.slow_consumer_alert(1200));
    }
}

#[test]
fn rejects_packet_too_large() {
    let cap = 1024;
    let mut backing = make_buffer(cap);
    let base = backing.as_mut_ptr();
    unsafe {
        let writer = RingWriter::new(base, cap).unwrap();
        let pkt = vec![0u8; SLOT_SIZE + 1];
        let err = writer.publish(&pkt).unwrap_err();
        matches!(err, RingError::PacketTooLarge { .. });
    }
}
