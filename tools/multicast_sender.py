import socket
import struct
import time

# SBE Header: BlockLength(u16), TemplateID(u16), SchemaID(u16), Version(u16)
# Little Endian
def create_sbe_packet(template_id=2, payload=b''):
    header = struct.pack('<HHHH', 16, template_id, 1, 0)
    return header + payload

def send_multicast(addr, port, count=10):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    
    # On Windows, binding to a specific interface for sending is implicit via routing table usually,
    # or we can bind.
    
    target = (addr, port)
    
    print(f"Sending {count} packets to {target}...")
    for i in range(count):
        # Create a dummy payload
        # Just some bytes
        payload = f"Packet {i}".encode('utf-8').ljust(100, b'\0')
        packet = create_sbe_packet(template_id=2, payload=payload)
        
        sock.sendto(packet, target)
        time.sleep(0.01)
        
    print("Done sending.")

if __name__ == "__main__":
    send_multicast("239.0.0.1", 5000)
