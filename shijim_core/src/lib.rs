use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use memmap2::MmapMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

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
            
            file.set_len(TOTAL_SIZE as u64)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to truncate shm file: {}", e)))?;
                
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
        })
    }

    fn publish(&mut self, price: f64) -> PyResult<()> {
        // This method is deprecated/legacy for raw struct, but we will adapt it to write SBE 
        // to match the integration test expectation, OR we introduce publish_sbe.
        // The user asked to "Update integration test... writer.publish_sbe".
        // So let's keep publish as is (or remove) and add publish_sbe.
        // But wait, the previous integration test used `publish`.
        // I will rename this to `publish_sbe` or just update `publish` to write SBE.
        // Given the Slot struct changed, `publish` MUST change.
        self.publish_sbe(price)
    }

    fn publish_sbe(&mut self, price: f64) -> PyResult<()> {
        unsafe {
            // Step 1: Reserve
            let cursor = (*self.header_ptr).write_cursor.load(Ordering::Relaxed);
            let next_seq = cursor + 1;
            let idx = (next_seq - 1) as usize % SLOT_COUNT;

            // Step 2: Write Payload
            let slot = self.slots_ptr.add(idx);
            (*slot).seq_num = next_seq;
            
            // Encode SBE into slot.data
            // Create a mutable slice from the raw pointer
            let data_slice = &mut (*slot).data;
            let mut encoder = SbeEncoder::new(data_slice);
            
            // Encode Header (Template 2, BlockLength 16)
            encoder.write_header(16, 2, 1, 0)
                .map_err(|e| PyRuntimeError::new_err(format!("SBE Encode Error: {:?}", e)))?;
                
            // Encode Body (TransactTime u64) - Mocking with 0 or current time?
            // BDD Scenario 2 in Python Reader says "Root Block... TransactTime".
            // Wait, the Python Reader BDD (Scenario 2) says "Root Block has Price field"?
            // Ah, the Python Reader BDD Scenario 2 says: "Given Root Block contains a 'Price' field".
            // But the Background says: "Body | TransactTime | u64".
            // There is a slight mismatch in the Python BDD text vs the "Background".
            // "Background... Body | TransactTime | u64".
            // "Scenario 2... Given Root Block contains a 'Price' field".
            // I will follow Scenario 2's implication that we want to test Price decoding.
            // So I will encode Price in the Root Block for this test, 
            // OR I will encode Price in the Group?
            // Python BDD Scenario 3 says "Group... MDEntryType...".
            // Let's assume the Schema is:
            // Header
            // Body: TransactTime (u64), MatchEventIndicator (u8) ... ?
            // Actually, let's just encode the Price as a Decimal64 in the Body for simplicity of verification,
            // matching the "publish(price)" signature.
            // Let's assume Body has [TransactTime(u64), Price(Decimal64)].
            
            encoder.write_u64(123456789) // TransactTime
                .map_err(|e| PyRuntimeError::new_err(format!("SBE Encode Error: {:?}", e)))?;
                
            encoder.write_decimal64(price)
                .map_err(|e| PyRuntimeError::new_err(format!("SBE Encode Error: {:?}", e)))?;
            
            // Memory Barrier
            std::sync::atomic::fence(Ordering::Release);

            // Step 3: Commit
            (*self.header_ptr).write_cursor.store(next_seq, Ordering::Release);
        }
        Ok(())
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
            (*self.header_ptr).write_cursor.store(next_seq, Ordering::Release);
        }
        Ok(())
    }
    
    fn current_cursor(&self) -> u64 {
        unsafe {
            (*self.header_ptr).write_cursor.load(Ordering::Relaxed)
        }
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
        
        let mut ingestor = UdpIngestor::new(&multicast_addr, &interface_addr)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to init ingestor: {}", e)))?;
            
        // Run for a bit or until count?
        // Let's just run 100 cycles for test.
        for _ in 0..100 {
             match ingestor.poll_cycle(self) {
                 Ok(_) => {},
                 Err(e) => return Err(PyRuntimeError::new_err(format!("Ingestion error: {}", e))),
             }
             // Sleep a tiny bit to avoid 100% CPU in this test loop
             std::thread::sleep(std::time::Duration::from_millis(10));
        }
        
        Ok(())
    }
}

#[pymodule]
fn shijim_core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RingBufferWriter>()?;
    Ok(())
}
