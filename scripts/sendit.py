import click
import socket
import json

samplejson = {
    "version": "1.0",
    "host": "example.org",
    "short_message": "A short message",
    "level": 5
}


@click.command()
@click.option('--udpport', default=10001, help='UDP port to send messages to.')
def main(udpport):
    config = {
        'udpport': udpport
    }
    click.echo(f"Configuration: {config}")

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        # Bind to the specified UDP port
        sock.bind(('0.0.0.0', udpport))
        click.echo(f"Bound to UDP port {udpport}")

        # Convert samplejson to JSON string
        message = json.dumps(samplejson).encode('utf-8')

        # Send the message (to localhost for demonstration)
        # Note: For actual sending, you'd specify a destination address
        sock.sendto(message, ('127.0.0.1', udpport))
        click.echo(f"Sent message: {samplejson}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
    finally:
        sock.close()


if __name__ == "__main__":
    main()