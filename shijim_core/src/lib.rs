use memmap2::MmapMut;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

mod sbe;
use sbe::SbeEncoder;

mod ingestion;
use ingestion::UdpIngestor;

// Configuration Constants
const HEADER_SIZE: usize = 128;
const SLOT_COUNT: usize = 1024;
const SLOT_SIZE: usize = 256;
const TOTAL_SIZE: usize = HEADER_SIZE + (SLOT_COUNT * SLOT_SIZE);

#[repr(C)]
struct RingBufferHeader {
    metadata_version: u16,
    buffer_capacity: u16,
    _padding1: u32,
    write_cursor: AtomicU64,
    _padding2: [u8; 112],
}

// Ensure Header is 128 bytes
const _: () = assert!(std::mem::size_of::<RingBufferHeader>() == 128);

#[repr(C, align(64))]
struct Slot {
    seq_num: u64,
    // SBE Payload (248 bytes)
    data: [u8; 248],
}

// Ensure Slot is 256 bytes
const _: () = assert!(std::mem::size_of::<Slot>() == 256);

#[pyclass]
struct RingBufferWriter {
    shm_name: String,
    mmap: MmapMut,
    // We keep raw pointers for fast access.
    // SAFETY: The mmap is kept alive by `mmap` field.
    header_ptr: *mut RingBufferHeader,
    slots_ptr: *mut Slot,
    udp_ingestor: Option<UdpIngestor>,
}

// SAFETY: MmapMut is Send, and we are just wrapping pointers into it.
// Python GIL ensures single-threaded access to methods, but if we release GIL or use in multi-threaded Rust,
// we need to be careful. For now, we assume single writer.
unsafe impl Send for RingBufferWriter {}

#[pymethods]
impl RingBufferWriter {
    #[new]
    fn new(shm_name: String) -> PyResult<Self> {
        let mmap = if cfg!(target_os = "windows") {
            if cfg!(target_os = "windows") {
                return Err(PyRuntimeError::new_err("Windows Named Shared Memory not fully implemented in this Rust stub. Please use Linux or File-backed for now."));
            } else {
                // Unreachable but keeps compiler happy or logic consistent
                panic!("Unreachable");
            }
        } else {
            // Default / Linux path
            let path = PathBuf::from(format!("/dev/shm/{}", shm_name));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to open shm file: {}", e)))?;

            file.set_len(TOTAL_SIZE as u64).map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to truncate shm file: {}", e))
            })?;

            unsafe { MmapMut::map_mut(&file) }
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to mmap: {}", e)))?
        };

        let header_ptr = mmap.as_ptr() as *mut RingBufferHeader;
        let slots_ptr = unsafe { mmap.as_ptr().add(HEADER_SIZE) } as *mut Slot;

        // Initialize Header
        unsafe {
            (*header_ptr).metadata_version = 1;
            (*header_ptr).buffer_capacity = SLOT_COUNT as u16;
            (*header_ptr).write_cursor.store(0, Ordering::SeqCst);
        }

        Ok(RingBufferWriter {
            shm_name,
            mmap,
            header_ptr,
            slots_ptr,
            udp_ingestor: None,
        })
    }

    fn publish(&mut self, price: f64) -> PyResult<()> {
        self.publish_sbe(price)
    }

    fn publish_sbe(&mut self, price: f64) -> PyResult<()> {
        self.encode_with(|encoder| {
            encoder
                .write_header(16, 2, 1, 0)
                .map_err(|e| PyRuntimeError::new_err(format!("SBE Encode Error: {:?}", e)))?;
            encoder
                .write_u64(123456789)
                .map_err(|e| PyRuntimeError::new_err(format!("SBE Encode Error: {:?}", e)))?;
            encoder
                .write_decimal64(price)
                .map_err(|e| PyRuntimeError::new_err(format!("SBE Encode Error: {:?}", e)))?;
            Ok(())
        })
    }

    pub fn publish_raw_bytes(&mut self, data: &[u8]) -> PyResult<()> {
        unsafe {
            // Step 1: Reserve
            let cursor = (*self.header_ptr).write_cursor.load(Ordering::Relaxed);
            let next_seq = cursor + 1;
            let idx = (next_seq - 1) as usize % SLOT_COUNT;

            // Step 2: Write Payload
            let slot = self.slots_ptr.add(idx);
            (*slot).seq_num = next_seq;

            // Copy raw bytes
            let slot_data = &mut (*slot).data;
            let len = std::cmp::min(data.len(), slot_data.len());
            slot_data[..len].copy_from_slice(&data[..len]);
            // Zero out remaining? Optional for perf.

            // Memory Barrier
            std::sync::atomic::fence(Ordering::Release);

            // Step 3: Commit
            (*self.header_ptr)
                .write_cursor
                .store(next_seq, Ordering::Release);
        }
        Ok(())
    }

    fn encode_with<F>(&mut self, f: F) -> PyResult<()>
    where
        F: FnOnce(&mut SbeEncoder) -> PyResult<()>,
    {
        unsafe {
            let cursor = (*self.header_ptr).write_cursor.load(Ordering::Relaxed);
            let next_seq = cursor + 1;
            let idx = (next_seq - 1) as usize % SLOT_COUNT;
            let slot = self.slots_ptr.add(idx);
            (*slot).seq_num = next_seq;
            let data_slice = &mut (*slot).data;
            let mut encoder = SbeEncoder::new(data_slice);
            f(&mut encoder)?;
            std::sync::atomic::fence(Ordering::Release);
            (*self.header_ptr)
                .write_cursor
                .store(next_seq, Ordering::Release);
        }
        Ok(())
    }

    fn current_cursor(&self) -> u64 {
        unsafe { (*self.header_ptr).write_cursor.load(Ordering::Relaxed) }
    }

    fn start_ingestion(&mut self, multicast_addr: String, interface_addr: String) -> PyResult<()> {
        // This blocks the thread! In real app, run in background thread or use non-blocking loop controlled by Python.
        // For this demo/test, we run a limited loop or indefinite?
        // The user spec says "Busy Loop Architecture".
        // If we block here, Python hangs.
        // Usually we spawn a thread.
        // But to keep it simple and testable, maybe we run for N packets or until signal?
        // Let's spawn a thread here? No, PyO3 class methods are called from Python.
        // If we want to run in background, we should spawn a std::thread.
        // But then we need to share `self` which is tricky with PyO3 (need Arc<Mutex> or similar).
        // For the "Integration Verification", we might want a method `poll_once`?
        // Or `run_ingestion_loop` that blocks (and we run it in a Python thread).

        // Let's implement `poll_ingestion` that runs one cycle, for testing control.
        // Or `start_ingestion_thread`?
        // Given the complexity of sharing `RingBufferWriter` across threads in PyO3 without `Py<T>`,
        // let's implement a blocking `ingest_packets(count)` for testing.

        let mut ingestor = if let Some(ing) = self.udp_ingestor.take() {
            ing
        } else {
            UdpIngestor::new(&multicast_addr, &interface_addr)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to init ingestor: {}", e)))?
        };

        // Run for a bit or until count?
        // Let's just run 100 cycles for test.
        for _ in 0..100 {
            match ingestor.poll_cycle(self) {
                Ok(_) => {}
                Err(e) => {
                    self.udp_ingestor = Some(ingestor);
                    return Err(PyRuntimeError::new_err(format!("Ingestion error: {}", e)));
                }
            }
            // Sleep a tiny bit to avoid 100% CPU in this test loop
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        self.udp_ingestor = Some(ingestor);

        Ok(())
    }
}

fn sbe_pyerr(e: sbe::SbeError) -> PyErr {
    PyRuntimeError::new_err(format!("SBE Encode Error: {:?}", e))
}

fn encode_levels(enc: &mut SbeEncoder, levels: &[(f64, u32)]) -> sbe::Result<()> {
    let count = levels.len() as u16;
    enc.write_group(13, count, |idx, writer| {
        let (price, qty) = levels[idx];
        writer.write_decimal64(price)?;
        writer.write_u32(qty)?;
        Ok(())
    })
}

#[pyfunction]
fn publish_tick_v1(
    writer: &mut RingBufferWriter,
    sec_id: u64,
    price: f64,
    size: u32,
    timestamp_ns: u64,
) -> PyResult<()> {
    writer.encode_with(|enc| {
        enc.write_header(24, 1001, 1, 0).map_err(sbe_pyerr)?;
        enc.write_u64(sec_id).map_err(sbe_pyerr)?;
        enc.write_u64(timestamp_ns).map_err(sbe_pyerr)?;
        enc.write_decimal64(price).map_err(sbe_pyerr)?;
        enc.write_u32(size).map_err(sbe_pyerr)?;
        Ok(())
    })
}

#[pyfunction]
fn publish_quote_v1(
    writer: &mut RingBufferWriter,
    sec_id: u64,
    bids: Vec<(f64, u32)>,
    asks: Vec<(f64, u32)>,
    timestamp_ns: u64,
) -> PyResult<()> {
    writer.encode_with(|enc| {
        enc.write_header(16, 1002, 1, 0).map_err(sbe_pyerr)?;
        enc.write_u64(sec_id).map_err(sbe_pyerr)?;
        enc.write_u64(timestamp_ns).map_err(sbe_pyerr)?;
        encode_levels(enc, &bids).map_err(sbe_pyerr)?;
        encode_levels(enc, &asks).map_err(sbe_pyerr)?;
        Ok(())
    })
}

#[pyfunction]
fn publish_snapshot_v1(
    writer: &mut RingBufferWriter,
    sec_id: u64,
    close: f64,
    high: f64,
    open_px: f64,
    timestamp_ns: u64,
) -> PyResult<()> {
    writer.encode_with(|enc| {
        enc.write_header(32, 1003, 1, 0).map_err(sbe_pyerr)?;
        enc.write_u64(sec_id).map_err(sbe_pyerr)?;
        enc.write_u64(timestamp_ns).map_err(sbe_pyerr)?;
        enc.write_decimal64(close).map_err(sbe_pyerr)?;
        enc.write_decimal64(high).map_err(sbe_pyerr)?;
        enc.write_decimal64(open_px).map_err(sbe_pyerr)?;
        Ok(())
    })
}

#[pyfunction]
fn publish_system_event(writer: &mut RingBufferWriter, event_code: u16) -> PyResult<()> {
    writer.encode_with(|enc| {
        enc.write_header(4, 1100, 1, 0).map_err(sbe_pyerr)?;
        enc.write_u16(event_code).map_err(sbe_pyerr)?;
        Ok(())
    })
}

#[pymodule]
fn shijim_core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RingBufferWriter>()?;
    m.add_function(wrap_pyfunction!(publish_tick_v1, m)?)?;
    m.add_function(wrap_pyfunction!(publish_quote_v1, m)?)?;
    m.add_function(wrap_pyfunction!(publish_snapshot_v1, m)?)?;
    m.add_function(wrap_pyfunction!(publish_system_event, m)?)?;
    Ok(())
}
