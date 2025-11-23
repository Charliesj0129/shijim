use socket2::{Socket, Domain, Type, Protocol};
use std::net::{SocketAddr, Ipv4Addr, UdpSocket};
use std::io;
use std::time::Duration;
use crate::RingBufferWriter;
use std::sync::atomic::Ordering;

pub struct UdpIngestor {
    socket: Socket,
    recv_buf: [u8; 1500], // Standard MTU
}

impl UdpIngestor {
    pub fn new(multicast_addr: &str, interface_addr: &str) -> io::Result<Self> {
        let addr: SocketAddr = multicast_addr.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let interface: Ipv4Addr = interface_addr.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        
        // Setup Reuse Addr/Port
        socket.set_reuse_address(true)?;
        #[cfg(not(windows))]
        socket.set_reuse_port(true)?; // Windows doesn't have SO_REUSEPORT, but SO_REUSEADDR is enough for multicast usually
        
        // Bind
        // For multicast, we bind to 0.0.0.0:PORT or the multicast address itself depending on OS.
        // Usually 0.0.0.0:PORT is safest for receiving from multiple interfaces.
        let bind_addr = SocketAddr::new("0.0.0.0".parse().unwrap(), addr.port());
        socket.bind(&bind_addr.into())?;
        
        // Join Multicast
        if let std::net::IpAddr::V4(mcast_v4) = addr.ip() {
            socket.join_multicast_v4(&mcast_v4, &interface)?;
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "IPv6 not supported yet"));
        }
        
        // Non-blocking
        socket.set_nonblocking(true)?;
        
        Ok(Self {
            socket,
            recv_buf: [0u8; 1500],
        })
    }
    
    pub fn poll_cycle(&mut self, writer: &mut RingBufferWriter) -> io::Result<bool> {
        // Use MaybeUninit for buffer? socket2 supports it. 
        // But for simplicity with safe Rust, we use initialized slice.
        // socket2's recv_from takes &mut [MaybeUninit<u8>].
        
        // We need to use socket2's recv_from which works with MaybeUninit, 
        // or convert our slice.
        // socket2 0.5+ changed API.
        // Let's use `recv_from` with `&mut [MaybeUninit<u8>]`.
        
        let mut buf = std::mem::MaybeUninit::new([0u8; 1500]);
        let buf_slice = unsafe { 
            std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut std::mem::MaybeUninit<u8>, 1500) 
        };
        
        match self.socket.recv_from(buf_slice) {
            Ok((size, _src)) => {
                // Got packet
                let packet = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, size) };
                
                // Filter Logic: Check TemplateID
                // SBE Header: BlockLength(u16), TemplateID(u16)
                if size >= 4 {
                    let template_id = u16::from_le_bytes([packet[2], packet[3]]);
                    
                    // Scenario 3: Filter Heartbeat (0)
                    if template_id == 0 {
                        return Ok(true); // Processed but ignored
                    }
                    
                    // Scenario 5: Truncation Check
                    // Slot Size is 256. If packet > 256, truncate or drop.
                    // Spec says: "Log warning and drop OR truncate".
                    // Let's truncate to fit slot for now, or drop if critical.
                    // RingBufferWriter writes to Slot.data which is [u8; 248].
                    // Wait, Slot Size is 256, but data payload is 248.
                    // Header (8 bytes) + Payload (248 bytes).
                    // If we write Raw Bytes, we are bypassing the SBE Encoder?
                    // Scenario 2 says: "Write 100 bytes to Slot".
                    // The RingBufferWriter currently has `publish_sbe` which encodes.
                    // We need a `publish_raw_bytes` for Passthrough mode.
                    
                    if size > 248 {
                        // Truncate or Drop
                        // eprintln!("Packet too large: {}", size);
                        // For now, truncate to 248
                        writer.publish_raw_bytes(&packet[..248])
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    } else {
                        writer.publish_raw_bytes(packet)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    }
                }
                
                Ok(true)
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // No packet
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }
}
