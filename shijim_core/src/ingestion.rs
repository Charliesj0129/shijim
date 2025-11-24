use crate::RingBufferWriter;
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};

pub struct UdpIngestor {
    socket: UdpSocket,
    recv_buf: [u8; 1500], // Standard MTU
}

impl UdpIngestor {
    pub fn new(multicast_addr: &str, interface_addr: &str) -> io::Result<Self> {
        let addr: SocketAddr = multicast_addr
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let interface: Ipv4Addr = interface_addr
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;

        let bind_addr = match addr {
            SocketAddr::V4(v4) => {
                if v4.ip().is_multicast() {
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), v4.port())
                } else {
                    SocketAddr::V4(v4)
                }
            }
            SocketAddr::V6(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "IPv6 not supported yet",
                ))
            }
        };
        socket.bind(&bind_addr.into())?;

        if let SocketAddr::V4(v4) = addr {
            if v4.ip().is_multicast() {
                socket.join_multicast_v4(v4.ip(), &interface)?;
            }
        }

        let udp_socket: UdpSocket = socket.into();
        udp_socket.set_nonblocking(true)?;

        Ok(Self {
            socket: udp_socket,
            recv_buf: [0u8; 1500],
        })
    }

    pub fn poll_cycle(&mut self, writer: &mut RingBufferWriter) -> io::Result<bool> {
        match self.socket.recv(&mut self.recv_buf) {
            Ok(size) => {
                let packet = &self.recv_buf[..size];

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
                        writer
                            .publish_raw_bytes(&packet[..248])
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    } else {
                        writer
                            .publish_raw_bytes(packet)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    }
                }

                Ok(true)
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(e) => Err(e),
        }
    }
}
