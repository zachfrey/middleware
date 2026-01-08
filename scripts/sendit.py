import click


@click.command()
@click.option('--udpport', default=514, help='UDP port to listen on.')

def main(udpport):
    config = {
        'udpport': udpport
    }
    click.echo(f"Configuration: {config}")
    

if __name__ == "__main__":
    main()