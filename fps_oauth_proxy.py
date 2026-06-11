import asyncio
import os


ALLOWED_CLIENT_IP = "154.132.13.118"
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = int(os.environ.get("SERVER_PORT", "8080"))


async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def handle_client(client_reader, client_writer):
    client_ip = client_writer.get_extra_info("peername")[0]
    if client_ip != ALLOWED_CLIENT_IP:
        print(f"Denied connection from {client_ip}", flush=True)
        client_writer.close()
        return

    try:
        request_line = await asyncio.wait_for(client_reader.readline(), timeout=15)
        method, target, _ = request_line.decode("latin-1").strip().split(" ", 2)

        headers = []
        while True:
            line = await asyncio.wait_for(client_reader.readline(), timeout=15)
            if line in (b"\r\n", b"\n", b""):
                break
            headers.append(line)

        if method.upper() != "CONNECT":
            client_writer.write(
                b"HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\n\r\n"
            )
            await client_writer.drain()
            client_writer.close()
            return

        host, port_text = target.rsplit(":", 1)
        remote_reader, remote_writer = await asyncio.open_connection(host, int(port_text))
        client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await client_writer.drain()

        await asyncio.gather(
            pipe(client_reader, remote_writer),
            pipe(remote_reader, client_writer),
        )
    except Exception as exc:
        print(f"Proxy error for {client_ip}: {exc}", flush=True)
        client_writer.close()


async def main():
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"OAuth proxy ready on {addresses}; allowed client: {ALLOWED_CLIENT_IP}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
